
var express = require('express');
var	app = express();
var http = require('http');
var server = http.createServer(app);
var port = process.env.PORT || 1212;
var io = require('socket.io').listen(server);
io.enable('browser client minification');  // send minified client
io.enable('browser client etag');          // apply etag caching logic based on version number
io.enable('browser client gzip'); 
server.listen(port);

if (process.env.REDISTOGO_URL) {
	var rtg   = require("url").parse(process.env.REDISTOGO_URL);
	var	bat_publisher = require("redis").createClient(rtg.port, rtg.hostname);
	var bowl_publisher = require("redis").createClient(rtg.port, rtg.hostname);
	var	cmi_publisher = require("redis").createClient(rtg.port, rtg.hostname);
	var score_publisher = require("redis").createClient(rtg.port, rtg.hostname);
	
	var bat_subscriber = require("redis").createClient(rtg.port, rtg.hostname);
	var bowl_subscriber = require("redis").createClient(rtg.port, rtg.hostname);
	var cmi_subscriber = require("redis").createClient(rtg.port, rtg.hostname);
	var score_subscriber = require("redis").createClient(rtg.port, rtg.hostname);
	
	bat_publisher.auth(rtg.auth.split(":")[1]);
	bowl_publisher.auth(rtg.auth.split(":")[1]);
	cmi_publisher.auth(rtg.auth.split(":")[1]);
	score_publisher.auth(rtg.auth.split(":")[1]);
	
	bat_subscriber.auth(rtg.auth.split(":")[1]);
	bowl_subscriber.auth(rtg.auth.split(":")[1]);
	cmi_subscriber.auth(rtg.auth.split(":")[1]);
	score_subscriber.auth(rtg.auth.split(":")[1]);
} else {
	var redis = require('redis');
	 bat_publisher = redis.createClient();
	 bowl_publisher = redis.createClient();
	 //cmi = clientkey matchkey and inning
	 cmi_publisher = redis.createClient();
	 score_publisher = redis.createClient();
	 bat_subscriber = redis.createClient();
	 bowl_subscriber = redis.createClient();
	 cmi_subscriber = redis.createClient();
	 score_subscriber = redis.createClient();
}


 bat_subscriber.subscribe('bat_score');
 bowl_subscriber.subscribe('bowl_score');
 cmi_subscriber.subscribe('cmi');
 score_subscriber.subscribe('score');

io.configure(function () { 
  io.set("transports", ["xhr-polling"]); 
  io.set("polling duration", 10); 
});

var pg = require('pg');
var conString =  process.env.DATABASE_URL || "tcp://postgres:password@localhost:5432/CricketAnalysis";
var _date = '01/01/2999';
var cmi = '';
var clientkey =0, matchkey=0, inning = 0;
var totalovers = 0.0, score = 0;

/*
pg.connect(process.env.DATABASE_URL, function(err, client) {
	checkDB(_date);
});

*/


var client = new pg.Client(conString);
client.connect(function(err) {
  console.log('*******************************'+err+'*************************'+_date+'***********************');
  client.query('SELECT NOW() AS "theTime"', function(err, result) {
      console.log(result.rows[0].theTime);
	  checkDB(_date,cmi);
      //output: Tue Jan 15 2013 19:12:47 GMT-600 (CST)
  });
});


function checkDB(_date, cmi){
		
		var cmi_arr = cmi.split('_');
		console.log(cmi_arr);
		if (cmi_arr[0] !=''){
			clientkey = parseInt(cmi_arr[0]);
			matchkey = parseInt(cmi_arr[1]);
			inning = parseInt(cmi_arr[2]);
		}
		var bat_query = client.query("select id, clientkey, matchkey, inning, batsmankey,batsman, position, fielder, outtype, bowler, runs, ballsfaced, zeros, strikerate, fours, sixes, hilite, nonstrikerkey from battingscorecards where updated_at > '" + _date+"'");
		var bowl_query = client.query("select id, clientkey, matchkey, inning, bowler, bowlerkey, overs, runs, maidens, wickets, economy, fours, sixes, wides, noballs, last_run, hilite, position, updated_at from bowlingscorecards where updated_at > '"+ _date+"'");
		var score_query = client.query("select clientkey, matchkey, inning, sum(wicketsgone) as wicketsgone, max(totalovers) as totalovers, sum(coalesce(runs,0)+coalesce(byes,0)+coalesce(legbyes,0)) as score from bowlingscorecards where clientkey="+ clientkey +" and matchkey="+matchkey+" and inning="+inning+" group by clientkey, matchkey, inning");
		bat_query.on('row', function(row){
			bat_publisher.publish('bat_score', JSON.stringify({id: row.id, matchkey:row.matchkey, inning:row.inning, clientkey:row.clientkey, batsmankey:row.batsmankey, batsman:row.batsman, position:row.position, fielder:row.fielder, outtype:row.outtype, bowler:row.bowler, runs:row.runs, ballsfaced:row.ballsfaced, zeros:row.zeros, strikerate:row.strikerate, fours:row.fours, sixes:row.sixes,hilite:row.hilite, nonstrikerkey:row.nonstrikerkey }));
		});
		bowl_query.on('row', function(row){
			bowl_publisher.publish('bowl_score', JSON.stringify({id: row.id, matchkey:row.matchkey, inning:row.inning, clientkey:row.clientkey, bowlerkey:row.bowlerkey, bowler:row.bowler, overs:row.overs, runs:row.runs, maidens:row.maidens, wickets:row.wickets, economy:row.economy, fours:row.fours, sixes:row.sixes, wides:row.wides, noballs:row.noballs, last_run:row.last_run, hilite:row.hilite, position:row.position }));
		});
		score_query.on('row', function(row){
			
			if  (row.totalovers>totalovers || row.score>score){
				score_publisher.publish('score', JSON.stringify({clientkey:row.clientkey, matchkey:row.matchkey, inning:row.inning,  score:row.score, wg:row.wicketsgone, to:row.totalovers}));
				totalovers = row.totalovers;
				score = row.score;
			}
			
		});
		var maxdate_query = client.query("SELECT max(to_char(updated_at, 'yyyy-mm-dd HH24:MI:SS.US')) as updated_at from bowlingscorecards");
		maxdate_query.on('row', function(row){
			
			if (row.updated_at == undefined){
				_date = '2999-01-01';
			}
			else{
				_date = row.updated_at;
			}
		});
		/*date_subscriber.on('message', function(channel, d){
			_date = d;
		});
		*/
		cmi_subscriber.on('message', function(channel, _cmi){
			cmi = _cmi;
		});
					
		setTimeout(function() {	
			checkDB(_date, cmi);
		}, 5000);
};

 
io.sockets.on('connection', function (socket) {
  io.sockets.emit('news', { will: 'be received by everyone'});

    bat_subscriber.on('message' , function(channel,bscorecard_object) {
		socket.emit('bat_score', JSON.parse(bscorecard_object));
	});
	
	bowl_subscriber.on('message' , function(channel,ballscorecard_object) {
		socket.emit('bowl_score', JSON.parse(ballscorecard_object));
	});
	
	score_subscriber.on('message' , function(channel,score_object) {
		socket.emit('score', JSON.parse(score_object));
	});
	
	/*	
	socket.on('max_date', function(date){
		console.log('this is the max date from the client');
		_date = date;
		date_publisher.publish('date', _date);
	});
	*/
	
	socket.on('cmi', function(_cmi){
		console.log('this is cmi ' +_cmi); 
		cmi_publisher.publish('cmi', _cmi);
	});
  socket.on('disconnect', function () {
	io.sockets.emit('user disconnected');
  });
});





