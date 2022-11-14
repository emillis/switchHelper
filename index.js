const fs = require("fs");

//This is analogous to job.createDataset, except, this allows passing on json object
//directly as a parameter which gets placed into the metadata. In order to do that,
//internally, the function creates a temporary file and uses it as the metadata which
//needs to be removed after "sendTo" is called so that there is no accumulation of
//unnecessary temp files. To do so, this function returns an object with method "remove()".
//This method needs to be called after any "sendTo" function in jobArrived.
async function CreateDataSet(job, datasetName, obj) {
    //Checking whether the right type of variables are supplied to the function
    if (typeof obj !== "object") {
        job.fail("Type of the variable supplied to createDataset function must be object!");
        return
    }
    if (typeof datasetName !== "string" || datasetName === "") {
        job.fail("DatasetName supplied to createDataset function must be string and at least one character long!");
        return
    }

    let tmpFile = {
        //Full path to the location where the json files is stored
        location: "",
        //Removes the files temporarily stored in the location
        remove: () => {
            fs.unlinkSync(tmpFile.location)
        },
        //Generates random name for the file and checks whether it's unique, if not, regenerates it
        generateNewFileName: () => {
            for (; ;) {
                tmpFile.location = "D:/Switch Scripts/_tmp_auto_removal_72h/tmp_" + Date.now() + "_" + Math.floor(Math.random() * 100000000000000000000000000000000000000) + ".json"

                if (fs.existsSync(tmpFile.location)) {
                    continue
                }

                break
            }
        },
    };

    tmpFile.generateNewFileName();

    fs.writeFileSync(tmpFile.location, JSON.stringify(obj))

    await job.createDataset(datasetName, tmpFile.location, DatasetModel.JSON);

    return tmpFile
}

function Test() {
    console.log(`WORKING!!!`);
}

module.exports = {
    Test
}