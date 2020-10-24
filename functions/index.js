const cio = require('cheerio');

const fetch = require('node-fetch')

const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();

const BASE_URL = 'https://dashboard.saskatchewan.ca';

const db = admin.firestore();

exports.fetchCaseStatistics = functions.https.onRequest(async (req, res) => {

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

    return res.send(newCases);
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

