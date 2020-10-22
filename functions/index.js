const cio = require('cheerio');

const fetch = require('node-fetch')

const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { _scheduleWithOptions } = require('firebase-functions/lib/providers/pubsub');

admin.initializeApp();

const BASE_URL = 'https://dashboard.saskatchewan.ca';

exports.fetchCaseStatistics = functions.https.onRequest(async (req, res) => {
    //https://dashboard.saskatchewan.ca/export/cases/1711.json
    // fetch(BASE_URL + '/health-wellness/covid-19/cases')
    //   .then((response) => response.text())
    //   .then((text) => {
    //     if (!_pageData) {
    //       _pageData = text;
    //       let $ = cio.load(_pageData);
    //       let test = $(".indicator-export > ul > li:contains('JSON')");
    //       _jsonURL = BASE_URL + test.find('a').attr('href');
    //       res.send(_jsonURL);
    //     }
    //   })
    //   .catch((error) => console.error(error))

    var _jsonURL;
    var data;
    await fetch(BASE_URL + '/health-wellness/covid-19/cases/')
        .then(res => res.text())
        .then(body => {
            let $ = cio.load(body);
            let jsonLink = $(".indicator-export > ul > li:contains('JSON')");
            _jsonURL = BASE_URL + jsonLink.find('a').attr('href');
            
        })
        .catch(error =>{
            console.error(error)
        });

    await fetch(_jsonURL)
        .then((response) => response.json())
        .then((json) => {
            data = json;
            
        })
        .catch((error) => console.error(error));

        processBulkCases(data);
    res.send(data);
    // getCases().then(data => {
    //     res.send(data);
    // });
});
/* idea:
-get last element in firestore
--get date from this element
-only add elements to the firestore where the date is larger than the last element
*/

function processBulkCases(rawData) {

    /*TODO: 
    -convert date to timestamp
    -convert text region to regionID
    -only find the cases that are missing.
    --easy way: only add the last 13 elements of the array
    --better way: get last element of the firestore and get date time.
                  add elements that have a larger date time.
    */
    let cleaned = rawData.map((item) => {
        //var temp = new Object();
        var temp = {};

        temp.date = item['Date'];
        temp.regionID = item['Region'];

        return temp;
        //return obj;
    });

    console.log(cleaned);
}

