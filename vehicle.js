var MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost:27017/vision2', function (err, db) {
    if (err) {
        console.log(err);
        return;
    }

    console.log('connected');
    var clients = db.collection('clients');
    var vehicles = db.collection('vehicles');
    vehicles.find().forEach(
        function (vehicle) {
            clients.findOne({ OldId: vehicle.ClientId }, function (err, client) {
                if (err) {
                    console.log(err);
                    return;
                }

                if (client) {
                    vehicles.updateOne({ _id: vehicle._id }, { $set: { ClientId: client._id } });
                    console.log(vehicle.ClientId + ' -> ' + client._id);
                }
            });
        },
        function (err) {
            if (err) {
                console.log(err);
            }

            db.close();
        }
    );
})
