const { DynamoDB, config } = require('aws-sdk');
const fs = require('fs');
const csv = require('csv-parser');

config.getCredentials(err => {
  if (err) {
    console.error(err.message);
  }
});

const dynamodb = new DynamoDB.DocumentClient({
  apiVersion: '2012-08-10',
  region: 'us-east-1',
});

const parseFile = filename => {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filename)
      .pipe(csv())
      .on('data', data => results.push(data))
      .on('end', () => {
        const mapped = results.map(result => ({
          id: parseInt(result.id, 10),
          kickoff: result.kickoff,
          home: 'TBD',
          away: 'TBD',
          home_goals: 0,
          away_goals: 0,
          home_penalties: 0,
          away_penalties: 0,
          venue: result.venue,
          stage: result.stage,
          scored: false,
          created_at: new Date().toDateString(),
        }));
        resolve(mapped);
      })
      .on('error', err => reject(err));
  });
};

async function loadMatches() {
  const data = await parseFile('./matches.csv');
  Promise.all(
    data.forEach(async item => {
      await dynamodb
        .put({
          TableName: 'matches',
          Item: {
            ...item,
          },
        })
        .promise();
    }),
  );
}

async function getMatch(id) {
  const item = await dynamodb
    .get({
      Key: {
        id,
      },
      TableName: 'matches',
    })
    .promise();

  return item;
}

async function listMatches() {
  const items = await dynamodb
    .scan({
      TableName: 'matches',
    })
    .promise();

  return items;
}

// loadMatches();

(async () => {
  await loadMatches();
  const match = await getMatch(15);
  const matches = await listMatches();
  console.log('🚀 ~ file: index.js ~ line 87 ~ matches', matches);
})();
