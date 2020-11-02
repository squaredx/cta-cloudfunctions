/*
project: cta-cloudfunctions
file: index.js
description: cloud functions required for the cta project for CS 372

Author: Jason Wolfe (200377485)
*/

//node libraries
const cio = require('cheerio');
const fetch = require('node-fetch')

//firebase libraries
const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();

const BASE_URL = 'https://dashboard.saskatchewan.ca';

const db = admin.firestore();


exports.fetchCaseStatistics = functions.pubsub.schedule('every 1 hours from 12:00 to 14:00').onRun(async (context) => {

    const newCases = await getNewCases();

    getMaxCases(newCases);

    return 'success';
});

// exports.testFn = functions.https.onRequest(async (req, res) => {
//     const newCases = await getNewCases();
//     //const newCases = JSON.parse(DUMMY_DATA);
//     getMaxCases(newCases);

    
//     res.send(newCases);
// });

/*
getRegionID(region)

    region: region in string form
    return: the corresponding region ID

    Description: used to transform the string representation of strings (as given in the govt data) to an integer
                 id. Since it is much easier to deal with ints compared to string.
*/
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
            console.log("WARN: regionID: " + region);
            break;
    }
    return result;
}

/*
getNewCases()
    return: an array of case objects corresponding to the new cases that were pushed to the cases collection

    Description: the function scrapes the govt of sask covid information website to find the URL to the JSON data
                 then the function retrieves this JSON data. With the data the function transforms it into a JS object
                 to submit to firebase. In order to reduce the amount of data sent to firestore, the function fetches the
                 date of the last case in firestore. It uses this date to filter out the new cases. At this point the
                 function submits the new cases into firestore using a batching function.
*/
async function getNewCases() {
    var _jsonURL; //hold the url path to the JSON data of the case data
    var data; //the json data fetched from the govt
    
    //await a fetch for the JSON URL from the government website 
    await fetch(BASE_URL + '/health-wellness/covid-19/cases/')
    .then(res => res.text())
    .then(body => {
        /* utilize the cheerio framework to scrape the website for the JSON url */
        let $ = cio.load(body);
        let jsonLink = $(".indicator-export > ul > li:contains('JSON')");
        _jsonURL = BASE_URL + jsonLink.find('a').attr('href'); //combine the base URL (govt website) with the path to the JSON data
        
    })
    .catch(error => console.error(error));

    //await a fetch for the JSON data from the govt website
    await fetch(_jsonURL)
    .then((response) => response.json())
    .then((json) => {
        data = json; //store the result in the data variable (so we can access through the function)
        
    })
    .catch((error) => console.error(error));

    
    //create a new map based on the data retrieved
    let transformed = data.map((item) => {
        var temp = {}; //create an empty object to store data

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

        return temp; //insert the resulting object into the 'transformed' array
    });

    //get the last case from firestore (based on date)
    let lastCase = await db.collection('cases').orderBy('date', 'desc').limit(1).get();
    let prevDate;

    if(!lastCase.empty)
        prevDate = lastCase.docs[0].data().date.toDate(); // set previous date to that which was retrieved from firestore
    else
        prevDate = new Date(2020, 1, 1); //else use an arbitrary date (pre covid in sask)

    newCases = transformed.filter(element => element.date.getTime() > prevDate.getTime()); //filter the array to only the cases that have a date later than the last date in the firestore

    console.log("there are " + newCases.length  + " new cases");

    //if there are less than 500 records that need to be uploaded (less than the firestore limit of 500 adds per second)
    if (newCases.length <= 500) {
        let batch = db.batch(); //create a batch transaction

        //for each object, create a document and set it in firestore and add to batch
        newCases.forEach(element => {
            let docRef = db.collection('cases').doc();
            batch.set(docRef, element);
        });

        batch.commit(); //commit the batch to firestore
    }
    else{
        //let totalRecords = newCases.length;
        let index = 0;

        let batch;
        
        /* same premise as above, but we need to create separate batches (of max 500 records per second) to avoid hitting
            transaction limits imposed by firestore 
        */

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

        //commit remaing cases
        if(index % 500 != 0) {
            console.log("Committing");
            batch.commit();
        }
    }

    //return the new cases (for use later)
    return newCases;
}


/*
getMaxCases(newCases)
    newCases: an array of case objects corresponding to the new cases that were pushed to the cases collection

    Description: the function finds the maximum amount of active cases for each region whenever it is called
                 the new cases are passed to the function and then the function compares the new data to the 
                 previously submitted data.

                 REQUIRES THE max-cases AND SUBSEQUENT 0-12 DOCUMENTS TO ALREADY EXIST!
*/
function getMaxCases(newCases) {
    //check to see if there are new cases.
    if(newCases.length == 0) {
        console.log("there are no new cases...not updating the max values");
        //since there are no new cases, exit the function
        return;
    }

    //get new cases for today by region
    let newData = new Array();
    //get the new active case numbers from the new batch of region information
    newCases.forEach((element) => {
        newData[element.regionID] = element.activeCases; //set the index (regionID) of newData to the active case numbers
    });

    //fetch the current stored 'max-cases' information from Firestore
    db.collection('max-cases').get().then((snapshot) => {
        let currentData = new Array(); //temporary array to store the retrieved data
        //loop over each document retrieved in the snapshot and insert it into the temp array according to the ID
        snapshot.forEach((doc) => {
            currentData[parseInt(doc.id)] = doc.data(); //doc.data is the object containing the region max-cases data
        });


        let regMax = 0; //temp var to store the max cases
        let regToday = false; //temp var to store if the max was achieved today
        //loop over the 13 regions
        for(var i = 0; i < 13; i ++){

            if(newData[i] > currentData[i].max) { //check if the newData from the new cases is a max for the region
                regMax = newData[i]; //set the max to the new data
                regToday = true; //set today to true
            }
            else { //else it is not a max
                regMax = currentData[i].max; //use the current max
                regToday = false; //set today to false
            }
            //update the document to the new data
            db.collection('max-cases').doc(i.toString()).set({
                max: regMax,
                today: regToday
            });
        }

    }).catch((error) => {
        //there was an error
        console.log("Unable to get current 'max-cases' data: ", error);
    });
}