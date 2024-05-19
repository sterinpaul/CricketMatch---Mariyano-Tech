require('dotenv').config();
const express = require('express');
const app = express();
const fs = require('fs').promises
const path = require('path')
const multer = require('multer')
const Joi = require('joi');

const port = 3000;
const upload = multer()
app.use(express.json())
app.use(express.urlencoded({ extended: true }))

const playersFilePath = path.join(__dirname, 'data', 'players.json')
const matchFilePath = path.join(__dirname, 'data', 'match.json')

// Database Details
const DB_USER = process.env['DB_USER'];
const DB_PWD = process.env['DB_PWD'];
const DB_URL = process.env['DB_URL'];
const DB_NAME = "task-sterin";
const DB_COLLECTION_NAME = "teams";

const { MongoClient, ServerApiVersion } = require('mongodb');
const uri = "mongodb+srv://" + DB_USER + ":" + DB_PWD + "@" + DB_URL + "/" + DB_NAME + "?retryWrites=true&w=majority&appName=freelancer";

const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

let db;

async function run() {
  try {
    await client.connect();
    await client.db("admin").command({ ping: 1 });

    db = client.db(DB_NAME);

    console.log("You successfully connected to MongoDB!");

  } finally {
  }
}


// Sample create document
async function sampleCreate() {
  const demo_doc = {
    "demo": "doc demo",
    "hello": "world"
  };
  const demo_create = await db.collection(DB_COLLECTION_NAME).insertOne(demo_doc);

  console.log("Added!")
  console.log(demo_create.insertedId);
}

const readJsonFile = async (path) => {
  try {
    const playersData = await fs.readFile(path, 'utf-8')
    return await JSON.parse(playersData)
  } catch (error) {
    console.log("Error occured while parsing json", error);
    return false
  }
}



// Endpoints

app.post('/add-team', upload.none(), async (req, res) => {
  try {
    const roles = ["WICKETKEEPER", "ALL-ROUNDER", "BOWLER", "BATTER"]
    const playersArray = await readJsonFile(playersFilePath)
    if (!playersArray.length) {
      return res.status(400).json({ status: false, message: "Error getting players data" })
    }
    const playerNames = playersArray.map(single => single.Player)

    const { teamName, captain, viceCaptain, players } = req.body
    const formData = { teamName, captain, viceCaptain }
    formData.players = [captain, viceCaptain, ...players]

    const objectSchema = Joi.object({
      teamName: Joi.string().min(3).max(25).required(),
      captain: Joi.string().required(),
      viceCaptain: Joi.string().required(),
      players: Joi.array().items(Joi.string().valid(...playerNames)).length(11).unique().required()
    })

    const { error, value } = objectSchema.validate(formData)

    if (error) {
      return res.status(400).json({ status: false, message: error.details[0].message })
    }

    const filteredPlayers = playersArray.filter(player => formData.players.includes(player.Player));
    const playerSelectionStatus = filteredPlayers.every(each => each.Team === filteredPlayers[0].Team)
    if (playerSelectionStatus) {
      return res.status(400).json({ status: false, message: "Maximum of 10 players only allowed from one team" })
    }

    const minMaxValidation = {}

    filteredPlayers.forEach(eachPlayerData => {
      if (minMaxValidation[eachPlayerData.Role]) {
        minMaxValidation[eachPlayerData.Role] = minMaxValidation[eachPlayerData.Role] + 1
      } else {
        minMaxValidation[eachPlayerData.Role] = 1
      }
    })

    const teamRoles = Object.keys(minMaxValidation)
    if (teamRoles.length !== 4) {
      let combinedRoles = ""
      const missingRole = roles.filter(each => !teamRoles.includes(each))
      if (missingRole.length > 1) {
        combinedRoles = missingRole.join(" & ")
      } else {
        combinedRoles = missingRole.join(" ")
      }
      return res.status(400).json({ status: false, message: `A team should contain role of a ${combinedRoles}` })
    }

    const saveTeam = { teamName, captain, viceCaptain }
    const mappedPlayers = filteredPlayers.map(eachPlayer => ({ player: eachPlayer.Player }))
    saveTeam.players = mappedPlayers
    const saveResponse = await db.collection(DB_COLLECTION_NAME).insertOne(saveTeam)

    if (saveResponse?.acknowledged) {
      return res.status(200).json({ status: true, message: "Team saved", data: value })
    }
    return res.status(400).json({ status: false, message: "Team could not be saved", data: value })

  } catch (error) {
    console.log("Error adding team", error);
    return res.status(500).json({ status: false, message: "Error adding team" })
  }
});

app.put('/process-result', async (req, res) => {
  try {
    const playersArray = await readJsonFile(playersFilePath)
    const matchArray = await readJsonFile(matchFilePath)
    if (!matchArray.length) {
      return res.status(400).json({ status: false, message: "Error getting match data" })
    }

    const players = {}
    matchArray.forEach(single => {
      if (!players[single.batter]) {
        players[single.batter] = {}
      }
      if (!players[single.bowler]) {
        players[single.bowler] = {}
      }
      if (single.fielders_involved !== "NA" && !players[single.fielders_involved]) {
        players[single.fielders_involved] = {}
      }
    })

    matchArray.forEach(ball => {

      if (players[ball.batter].totalRun) {
        players[ball.batter].totalRun += ball.batsman_run
      } else {
        players[ball.batter].totalRun = ball.batsman_run
      }

      if (ball.batsman_run === 4) {
        if (players[ball.batter].boundary) {
          players[ball.batter].boundary += 1
        } else {
          players[ball.batter].boundary = 1
        }
      }

      if (ball.batsman_run === 6) {
        if (players[ball.batter].sixes) {
          players[ball.batter].sixes += 1
        } else {
          players[ball.batter].sixes = 1
        }
      }

      if (ball.isWicketDelivery) {
        if (players[ball.bowler].wickets) {
          players[ball.bowler].wickets += 1
        } else {
          players[ball.bowler].wickets = 1
        }
      }

      if (ball.kind === "caught") {
        if (players[ball.fielders_involved].caught) {
          players[ball.fielders_involved].caught += 1
        } else {
          players[ball.fielders_involved].caught = 1
        }
      }

      if (ball.kind === "caught and bowled") {
        if (players[ball.bowler].caught) {
          players[ball.bowler].caught += 1
        } else {
          players[ball.bowler].caught = 1
        }
      }
      if (ball.kind === "lbw" || ball.kind === "bowled") {
        if (players[ball.bowler].lbwAndBowled) {
          players[ball.bowler].lbwAndBowled += 1
        } else {
          players[ball.bowler].lbwAndBowled = 1
        }
      }

      if (players[ball.bowler].overs) {
        if (players[ball.bowler].overs[ball.overs]) {
          players[ball.bowler].overs[ball.overs] += ball.total_run
        } else {
          players[ball.bowler].overs[ball.overs] = ball.total_run
        }
      } else {
        players[ball.bowler].overs = {}
        players[ball.bowler].overs[ball.overs] = ball.total_run
      }
    })
    // console.log('result', players);


    const updatedScores = []
    for (const key in players) {

      const player = {
        player: key,
        point: 0
      }

      if (players[key].totalRun) {
        player.point += players[key].totalRun

        if (players[key].totalRun === 0) {
          const roleCheck = playersArray.find(single => single.Player === key)
          if (roleCheck.Role !== "BOWLER") {
            player.point -= 2
          }
        }
        if (players[key].boundary) {
          player.point += players[key].boundary
        }
        if (players[key].sixes) {
          player.point += (players[key].sixes * 2)
        }
        if (players[key].totalRun > 29) {
          player.point += 4
        }
        if (players[key].totalRun > 99) {
          const count = Math.floor(players[key].totalRun / 100)
          player.point += count * 16
        }
        if (players[key].totalRun > 49) {
          const count = Math.floor(players[key].totalRun % 100 / 50)
          player.point += count * 8
        }
      }

      if (players[key].lbwAndBowled) {
        player.point += players[key].lbwAndBowled * 8
      }
      if (players[key].wickets) {
        player.point += players[key].wickets * 25
        if(players[key].wickets === 3){
          player.point += 4
        }
        if(players[key].wickets === 4){
          player.point += 8
        }
        if(players[key].wickets === 5){
          player.point += 16
        }
      }
      if(players[key].overs){
        const eachPlayerOver = players[key].overs
        let maidenOverCount = 0
        for(const objKey in eachPlayerOver){
          if(eachPlayerOver[objKey] === 2){
            maidenOverCount++
          }
        }
        if(maidenOverCount){
          player.point += maidenOverCount * 12
        }
      }
      if(players[key].caught){
        player.point += players[key].caught * 8
        if(players[key].caught === 3){
          player.point += 4
        }
      }
      updatedScores.push(player)
    }
    
    const queryArray = []
    updatedScores.forEach(eachPlayerScore => {
      queryArray.push(
        db.collection(DB_COLLECTION_NAME).updateMany(
          { "players.player": eachPlayerScore.player },
          [
            {
              $set: {
                players: {
                  $map: {
                    input: "$players",
                    as: "player",
                    in: {
                      $cond: [
                        { $eq: ["$$player.player", eachPlayerScore.player] },
                        {
                          $mergeObjects: [
                            "$$player",
                            {
                              point: {
                                $cond: [
                                  { $eq: ["$captain", eachPlayerScore.player] },
                                  2 * eachPlayerScore.point,
                                  {
                                    $cond: [
                                      { $eq: ["$viceCaptain", eachPlayerScore.player] },
                                      1.5 * eachPlayerScore.point,
                                      eachPlayerScore.point
                                    ]
                                  }
                                ]
                              }
                            }
                          ]
                        },
                        "$$player"
                      ]
                    }
                  }
                }
              }
            },
            {
              $set: {
                total: {
                  $sum: "$players.point"
                }
              }
            }
          ]
        )
      );
    });
    
    const bulkUpdate = await Promise.allSettled(queryArray)
    const bulkValidation = bulkUpdate.every(each=>each.status === 'fulfilled')
    if(bulkValidation){
      return res.status(200).json({ status: true , message: "Result Processed successfully"})
    }
    return res.status(400).json({ status: false , message: "Result processing failed"})

  } catch (error) {
    console.log("Error processing result", error);
    return res.status(500).json({ status: false, message: "Error processing the result" })
  }
});

app.get('/team-result', async (req, res) => {
  try {
    const saveResponse = await db.collection(DB_COLLECTION_NAME).find().sort({total:-1}).toArray();
    if(saveResponse.length){
      return res.status(200).json({ status: true , message: "Result found successfully",data: saveResponse})
    }
    return res.status(400).json({ status: false , message: "No teams found"})
  } catch (error) {
    console.log("Error getting team results", error);
    return res.status(500).json({ status: false, message: "Error getting team results" })
  }
});


app.listen(port, () => {
  console.log(`App listening on port ${port}`);
});

run();