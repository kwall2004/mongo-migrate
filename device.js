var MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost:27017/Vision', function (err, db) {
    if (err) {
        console.log(err);
        return;
    }

    console.log('connected');
    var clients = db.collection('Client');
    var devices = db.collection('Device');
    devices.find().forEach(
        function (device) {
            clients.findOne({ OldId: device.ClientId }, function (err, client) {
                if (err) {
                    console.log(err);
                    return;
                }

                if (client) {
                    devices.updateOne({ _id: device._id }, { $set: { ClientId: client._id } });
                    console.log(device.ClientId + ' -> ' + client._id);
                }
            });

            devices.updateOne({ _id: device._id }, { $set: { IMEI: device.IMEI.toString() }});
            devices.updateOne({ _id: device._id }, { $set: { SerialNumber: device.SerialNumber.toString() }});
            devices.updateOne({ _id: device._id }, { $set: { GroupId: parseInt(device.GroupId) }});
        },
        function (err) {
            if (err) {
                console.log(err);
            }

            db.close();
        }
    );
})
