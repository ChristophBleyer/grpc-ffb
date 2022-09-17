'use strict';

const fs = require("fs");
const process = require('process');

const grpc = require('grpc');
const PROTO_PATH = "../pb/messages.proto";
const serviceDef = grpc.load(PROTO_PATH)

const PORT = 9000

const client = new serviceDef.EmployeeService(`localhost:${PORT}`, grpc.credentials.createInsecure())

const option = parseInt(process.argv[2], 10)

switch (option) {
    case 1:
        sendMetadata(client);
        break;
    case 2:
        getByBadgeNumber(client);
        break;
    case 3:
        getAll(client);
        break;
    case 4:
        addPhoto(client);
        break;
    case 5:
        saveAll(client);
        break
}

function sendMetadata(client) {
    const md = new grpc.Metadata();
    md.add("username", "christoph")
    md.add("message", "hey")

    client.getByBadgeNumber({}, md, function (err, response) { // Params: RequestObject, Metadata -> optional, Callback
        if (err) {
            console.log(err)
        } else {
            console.log(response)
        }
    });
}

function getByBadgeNumber(client) {
    client.getByBadgeNumber({badgeNumber: 2080}, function (err, response) {
        if (err) { // err as values like in go...
            console.log(err);
        } else {
            console.log(response.employee);
        }
    });
}

function getAll(client) {
    const call = client.GetAll({}, ) // Server Side Streaming: Client inits the conn

    call.on("data", function (data) {
        console.log(data.employee);
    });
}

function addPhoto(client) {
    const md = new grpc.Metadata(); // send correlation only once not with every byte chunk
    md.add("badgenumber", "2080");

    const call = client.addPhoto(md, function(err, result) {
        if (err) {
            console.log(err);
        } else {
            console.log(result);
        }
    });

    const stream = fs.createReadStream("gopher.png")
    stream.on("data", function(chunk) { // stream the image in byte chunks
        call.write({data: chunk});
    });

    stream.on("end", function () { // once the image is read notify the server we are done
        call.end();
    });
}

function saveAll(client) {
    const employees = [
        {id: 4,
        badgeNumber: 123,
        firstName: "John",
        lastName: "Smith",
        vacationAccrualRate: 1.2,
        vacationAccrued: 9,
        },
        {id: 5,
            badgeNumber: 88,
            firstName: "Phil",
            lastName: "MM",
            vacationAccrualRate: 120,
            vacationAccrued: 900,
        },
    ];

    const call = client.saveAll(); // opens the channel to the server
    call.on("data", function(emp) { // log receive. Should be registered before sending the first message
        console.log(emp.employee)
    });
    employees.forEach(function(emp) { // Send on the server is blocked until we write first
        call.write({employee: emp});
    });
    call.end(); // close the channel once done
}