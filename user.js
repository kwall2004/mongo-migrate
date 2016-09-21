var MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost:27017/Vision', function (err, db) {
    if (err) {
        console.log(err);
        return;
    }

    console.log('connected');
    var clients = db.collection('Client');
    var users = db.collection('User');
    users.find().forEach(
        function (user) {
            clients.findOne({ OldId: user.ClientId }, function (err, client) {
                if (err) {
                    console.log(err);
                    return;
                }

                if (client) {
                    users.updateOne({ _id: user._id }, { $set: { ClientId: client._id } });
                    console.log(user.ClientId + ' -> ' + client._id);
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
