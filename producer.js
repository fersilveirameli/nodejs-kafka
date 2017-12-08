const kafka = require('kafka-node');
const Producer = kafka.Producer;
const KeyedMessage = kafka.KeyedMessage;
const Client = kafka.Client;
const client = new Client();
const argv = require('optimist').argv;
const topic = argv.topic || 'topic1';
const p = argv.p || 0;
const a = argv.a || 0;
const producer = new Producer(client, { requireAcks: 1 });

producer.on('ready', function () {

    console.log('ready');
    const message = {
        number:'1234',
        airline: 'YZ',
        boardpoint: 'FLN',
        offpoint: 'GRU'
    };


    producer.send([
        { topic: topic, messages: JSON.stringify(message) }
    ], function (err, result) {
        if(err) console.log('Err: '+err);
        console.log(result);
        process.exit();
    });
});

producer.on('error', function (err) {
    console.log('error', err);
});
