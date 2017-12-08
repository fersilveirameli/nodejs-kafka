'use strict';

var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic || 'pnae';

var client = new Client();
var topics = [
    {topic: topic, partition: 0}
];
var options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024
};

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
    console.log(this.id, message);
});

consumer.on('error', function (err) {
    console.log('error', err);
});


consumer.on('offsetOutOfRange', function (topic) {
    console.log("------------- offsetOutOfRange ------------");    
});
