
mass = require('./build/Release/node-masstree')
//mass = require('./build/Debug/node-masstree')

//mass.sendPutCol("test", 0, "a", 0); mass.flush(); mass.receive()


var start = process.hrtime();

var elapsed_time = function(note){
    var precision = 3; // 3 decimal places
    var elapsed = process.hrtime(start)[1] / 1000000; // divide by a million to get nano to milli
    console.log(process.hrtime(start)[0] + " s, " + elapsed.toFixed(precision) + " ms - " + note); // print message + time
    start = process.hrtime(); // reset the timer
}
var seq = 0;
var size = 100000;
console.log("Using", size, "keys!");


//Whole
console.log("Write")
////pipe

for(var i=0;i<size;i++) {
    mass.sendPutWhole("test"+i, i.toString(), seq++);
}
mass.flush();
for(var i=0;i<size;i++) {
  mass.receive();
}
elapsed_time("Whole pipe")

////pipe json
for(var i=0;i<size;i++) {
  mass.sendPutWhole("test"+i, i.toString(), seq++);
}
mass.flush();
for(var i=0;i<size;i++) {
  JSON.parse(mass.receiveJSON());
}
elapsed_time("Whole pipe json")

//single

for(var i=0;i<size;i++) {
  mass.putWhole("test"+i, i.toString(), seq++)
}

elapsed_time("Whole single")

for(var i=0;i<size;i++) {
  JSON.parse(mass.putWholeJSON("test"+i, i.toString(), seq++))
}

elapsed_time("Whole single json")

return;


for(var i=0;i<size;i++) {
    mass.sendGetWhole("test"+i, seq++);
}

mass.flush();

for(var i=0;i<size;i++) {
  mass.receive()
}

elapsed_time("getting each")


for(var i=0;i<size;i+=10) {
  mass.sendScanWhole("test"+i, 10, seq++)
}

mass.flush();

for(var i=0;i<size;i+=10) {
  mass.receive();
}

elapsed_time("scanning 10 at a time")

for(var i=0;i<size;i+=100) {
    mass.sendScanWhole("test"+i, 100, seq++)
}

mass.flush();

for(var i=0;i<size;i+=100) {
  mass.receive();
}

elapsed_time("scanning 100 at a time")

mass.sendScanWhole("test0", size, seq++)
mass.flush();
x = mass.receive();

elapsed_time("getting all in single scan")


mass.checkpoint();

