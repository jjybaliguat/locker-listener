const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');
require('dotenv').config();

// MQTT broker settings
const scanTopic = 'locker/scan';
const unlockTopic = 'locker/unlock';
const deniedTopic = 'locker/denied';
const lockTopic = 'locker/lock';

// MongoDB URI and client
const mongoUri = process.env.MONGODB_URI;
const client = mqtt.connect(process.env.MQTT_BROKER, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
  clientId: 'locker-listener',
});

// Mongo client setup
const mongoClient = new MongoClient(mongoUri);
let db;

async function findInstructorWithLocker(qrCode) {
    await connectMongo()
  try {
    const instructor = await db.collection(process.env.MONGODB_COLLECTION).aggregate([
      {
        $match: { qrCode: qrCode }
      },
      {
        $lookup: {
          from: "Locker",                      // Collection to join
          localField: "_id",                   // InstructorProfile._id
          foreignField: "instructorId",        // Locker.instructorId
          as: "locker"
        }
      },
      {
        $unwind: {
          path: "$locker",
          preserveNullAndEmptyArrays: true    // Optional: keep instructors without lockers
        }
      }
    ]).next();

    return instructor;

  } catch (err) {
    console.error("❌ Error fetching instructor:", err);
  } finally {
    await mongoClient.close();
  }
}

/**
 * Update the status of a locker by lockerNumber
 * @param {string|number} lockerNumber - The locker number to update
 * @param {string} status - New status (e.g., "LOCKED", "UNLOCKED")
 */

async function updateLockerStatus(lockerNumber, status = "LOCKED") {
  try {
    await connectMongo()
    const lockerCollection = db.collection("Locker");

    const lockerNum = parseInt(lockerNumber, 10);
    const result = await lockerCollection.updateOne(
      { lockerNumber: lockerNum },
      { $set: { status, updatedAt: new Date() } }
    );

    if (result.modifiedCount > 0) {
      console.log(`🔒 Locker ${lockerNum} status updated to "${status}".`);
    } else {
      console.warn(`⚠️ Locker ${lockerNum} not found or status unchanged.`);
    }
  } catch (error) {
    console.error('❌ Failed to update locker status:', error);
  } finally {
    await mongoClient.close();
  }
}

async function connectMongo() {
  try {
    await mongoClient.connect();
    db = mongoClient.db('QRLocker');
    console.log('✅ Connected to MongoDB');
  } catch (err) {
    console.error('❌ MongoDB connection error:', err);
  }
}

client.on('connect', async () => {
  console.log('✅ Connected to MQTT broker');
  await connectMongo();

  client.subscribe(scanTopic, (err) => {
    if (err) {
      console.error('❌ Failed to subscribe to scan topic:', err);
    } else {
      console.log(`📡 Listening for QR codes on topic: ${scanTopic}`);
    }
  });
  client.subscribe(lockTopic, (err) => {
    if (err) {
      console.error('❌ Failed to subscribe to scan topic:', err);
    } else {
      console.log(`📡 Listening on topic: ${lockTopic}`);
    }
  });
});

client.on('message', async (topic, message) => {
    if(topic === lockTopic){
        const lockerNumberStr = message.toString().trim(); // e.g. "5"
        const lockerNumber = parseInt(lockerNumberStr, 10); // Convert to number if needed
        await updateLockerStatus(lockerNumber, "LOCKED")
    }
  if (topic === scanTopic) {
    const qrCode = message.toString().trim();
    console.log(`📥 QR Code received: ${qrCode}`);

    try {
      const instructor = await findInstructorWithLocker(qrCode);

    //   console.log(instructor)

      if (instructor && instructor.locker) {
        const lockerNum = parseInt(instructor.locker?.lockerNumber, 10);

        if (lockerNum >= 1 && lockerNum <= 15) {
          const payload = JSON.stringify({ locker: lockerNum });
          client.publish(unlockTopic, payload);
          updateLockerStatus(lockerNum, "UNLOCKED");
          console.log(`✅ Access granted. Sent unlock for locker ${lockerNum}`);
        } else {
          console.warn('⚠️ Invalid locker number in DB entry');
        }
      } else {
        console.log('🚫 Access denied. QR code not authorized.');
        client.publish(deniedTopic, "Access denied. QR code not authorized.");
      }
    } catch (err) {
      console.error('❌ Database query failed:', err.message);
    }
  }
});
