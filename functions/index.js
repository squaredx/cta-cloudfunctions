const cio = require('cheerio');

const fetch = require('node-fetch')

const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();

const BASE_URL = 'https://dashboard.saskatchewan.ca';

const db = admin.firestore();

//functions.https.onRequest(async (req, res) =>
exports.fetchCaseStatistics = functions.pubsub.schedule('every 1 hours from 13:00 to 15:00').onRun(async (context) => {

    const newCases = await getNewCases();

    getNewCases(newCases);

    return 'success';
    //return res.send(newCases);
});

exports.testFn = functions.https.onRequest(async (req, res) => {
    const newCases = await getNewCases();

    //console.log(newCases);
    await getMaxCases(newCases);

    generateMaxCases();
    res.send('yes');
});



//TODO: some how display and error if -1 is returned (for our purposes)
function getRegionID(region) {
    let result;
    switch (region) {
        case "Far North West":
            result = 0;
            break;
        case "Far North Central":
            result = 1;
            break;
        case "Far North East":
            result = 2;
            break;
        case "North West":
            result = 3;
            break;
        case "North Central":
            result = 4;
            break;
        case "North East":
            result = 5;
            break;
        case "Saskatoon":
            result = 6;
            break;
        case "Central West":
            result = 7;
            break;
        case "Central East":
            result = 8;
            break;
        case "Regina":
            result = 9;
            break;
        case "South West":
            result = 10;
            break;
        case "South Central":
            result = 11;
            break;
        case "South East":
            result = 12;
            break;
        default:
            result = -1;
            break;
    }
    return result;
}

async function getNewCases() {
    var _jsonURL;
    var data;
    
    await fetch(BASE_URL + '/health-wellness/covid-19/cases/')
    .then(res => res.text())
    .then(body => {
        let $ = cio.load(body);
        let jsonLink = $(".indicator-export > ul > li:contains('JSON')");
        _jsonURL = BASE_URL + jsonLink.find('a').attr('href');
        
    })
    .catch(error => console.error(error));

    await fetch(_jsonURL)
    .then((response) => response.json())
    .then((json) => {
        data = json;
        
    })
    .catch((error) => console.error(error));



    let transformed = data.map((item) => {
        var temp = {};

        //Convert the string date to a Date object
        var dateParts = item['Date'].split("/");
        temp.date = new Date(dateParts[0], dateParts[1] - 1, dateParts[2]); //month is from 0-11 so minus 1

        //get the regionID for the specified region
        temp.regionID = getRegionID(item['Region']);

        //No tranformations needed for case numbers, so just transfer the info over
        temp.newCases = item['New Cases'];
        temp.totalCases = item['Total Cases'];
        temp.activeCases = item['Active Cases'];
        temp.inpatientHospitalizations = item['Inpatient Hospitalizations'];
        temp.icuHospitalizations = item['ICU Hospitalizations'];
        temp.recoveredCases = item['Recovered Cases'];
        temp.deaths = item['Deaths'];

        return temp;
    });

    let casesRef = db.collection('cases');

    let lastCase = await casesRef.orderBy('date', 'desc').limit(1).get();
    let prevDate;

    if(!lastCase.empty)
        prevDate = lastCase.docs[0].data().date.toDate();
    else
        prevDate = new Date(2020, 1, 1);

    newCases = transformed.filter(element => element.date.getTime() > prevDate.getTime());

    console.log("there are " + newCases.length  + " new cases");

    if (newCases.length <= 500) {
        let batch = db.batch();

        newCases.forEach(element => {
            let docRef = db.collection('cases').doc();
            batch.set(docRef, element);
        });

        batch.commit();
    }
    else{
        //let totalRecords = newCases.length;
        let index = 0;

        let batch;
        
        newCases.forEach(element => {
            if(index % 500 == 0){
                if(index > 0){
                    
                    console.log("Committing");
                    batch.commit();
                }
                batch = db.batch();
            }

            index++;

            let docRef = db.collection('cases').doc();
            batch.set(docRef, element);
        });

        if(index % 500 != 0) {
            console.log("Committing");
            batch.commit();
        }
    }

    return newCases;
}

async function getMaxCases(newCases) {

    //get new cases for today by region
    let newData = new Array();

    newCases.forEach((element) => {
        newData[element.regionID] = element.newCases;
    });

    db.collection('max-cases').get().then((snapshot) => {
        let currentData = new Array()
        snapshot.forEach((doc) => {
            currentData[parseInt(doc.id)] = doc.data();
        });
        console.log(currentData);


        let regMax = 0;
        let regToday = false;
        for(var i = 0; i < 13; i ++){
            
            if(newData[i] > currentData[i].max) {
                regMax = newData[i];
                regToday = true;
            }
            else {
                regMax = currentData[i].max;
                regToday = false;
            }

            db.collection('max-cases').doc(i.toString()).set({
                max: regMax,
                today: regToday
            });
        }

    });

    //Get the current max values from firebase
    //compare them to the new reports from today.
    //filter out the ids who have new highs
    //create a batch of updates
    //set a bool of new high
    //for those who did not have a new high, set bool to false.
}


function generateMaxCases() {
    let batch = db.batch();

    let template = {};

    template.max = 0;
    template.today = false;

    for(var i = 0; i < 13; i ++) {
        let docRef = db.collection('max-cases').doc(i.toString());
        batch.set(docRef, template);
    }

    batch.commit(); 
}