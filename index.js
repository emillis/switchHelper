const fs = require("fs");
const path = require("path");
const excel = require("xlsx")

//Reads environmental variable passed in (which is supposed to point to a Switch Config JSON file), reads
//the file and returns as JSON object
function GetGlobalSwitchConfig(env_var = "SwitchConfig") {
    const loc = process.env[env_var]

    if (!loc) {
        throw Error(`Environmental variable "${env_var}" is not set!`)
    }

    if (path.parse(loc).ext !== ".json") {
        throw Error(`Path to global settings for switch "${loc}" defined in ENV variable "${env_var}" does not point to a JSON file!`)
    }

    return JSON.parse(fs.readFileSync(loc, "utf-8"))
}

//Generates a date string in the following format: 20221011103552333. You can also define the separator.
//If you chose ".", the date would look like this:  2022.10.11.10.35.52.333. You cal also omit the milliseconds
//in which case it will look the same except without the segment
function GenerateDateString(separator = "", includeMs = true) {
    let d = new Date()

    const YYYY = d.getUTCFullYear(),
        MM = separator+(d.getUTCMonth() < 10 ? `0${d.getUTCMonth()}` : d.getUTCMonth()),
        DD = separator+(d.getUTCDate() < 10 ? `0${d.getUTCDate()}` : d.getUTCDate()),
        hh = separator+(d.getUTCHours() < 10 ? `0${d.getUTCHours()}` : d.getUTCHours()),
        mm = separator+(d.getUTCMinutes() < 10 ? `0${d.getUTCMinutes()}` : d.getUTCMinutes()),
        ss = separator+(d.getUTCSeconds() < 10 ? `0${d.getUTCSeconds()}` : d.getUTCSeconds()),
        ms = includeMs ? separator+d.getUTCMilliseconds() : ""

    return `${YYYY}${MM}${DD}${hh}${mm}${ss}${ms}`
}

//Generating a new name perfect for using with creation of random files. Note: existing file check should still
//be performed as there's a micro chance to produce a duplicate name
function GenerateNewName(prefix = "", suffix = "", separator = "_") {
    return `${prefix ? prefix+separator : ""}${GenerateDateString()}${separator}${Math.round(Math.random() * 1000000000000)}${suffix ? separator+suffix : ""}`
}

//This is analogous to job.createDataset, except, this allows passing on json object
//directly as a parameter which gets placed into the metadata. In order to do that,
//internally, the function creates a temporary file and uses it as the metadata.
async function CreateDataSet(job, datasetName, obj, tmp_file_store) {
    if (!tmp_file_store) {
        tmp_file_store = GetGlobalSwitchConfig()["TempMetadataFileLocation"];
        if (!tmp_file_store) {
            throw Error(`Location where to store temporary created files could not been found for dataset name "${datasetName}"!`)
        }
    }
    //Checking whether the right type of variables are supplied to the function
    if (typeof obj !== "object") {
        throw Error(`Expected to receive data type "object", got "${typeof obj}"    . Dataset can only be created from an object`)
    }
    if (typeof datasetName !== "string" || datasetName === "") {
        throw Error(`Dataset name "${datasetName.toString()}" is invalid!`)
    }
    if (!tmp_file_store || !fs.existsSync(tmp_file_store)) {
        throw Error(`Invalid location "${tmp_file_store}" for storing temporary metadata files!`)
    }

    let location;

    for (;;) {
        location = path.join(tmp_file_store, `${GenerateNewName("dataset")}.json`);

        if (fs.existsSync(location)) {continue}

        break
    }

    fs.writeFileSync(location, JSON.stringify(obj))

    await job.createDataset(datasetName, location, "JSON");

    return {
        removeTmpFiles: function () {
            try { fs.unlinkSync(location) } catch {}
        }
    }
}

//Checking whether dataset exists
async function DataSetExists(job, name) {
    try {
        for (let set of await job.listDatasets()) {
            if (set["name"] !== name) {
                continue
            }

            return true
        }
    } catch (e) {
        await job.log("warning", e.toString());
        return false
    }

    return false
}

//Returns dataset as JSON object
async function GetDataSet(job, name) {
    try {
        if (!await DataSetExists(job, name)) {
            throw Error(`Dataset "${name}" does not exist!`)
        }

        return JSON.parse(fs.readFileSync(await job.getDataset(name, "readOnly"), "utf-8"));
    } catch (e) {
        await job.log("warning", e.toString());
        throw e.toString()
    }
}

//Returns property value if name exist or undefined if it doesn't
async function GetProperty(flowElement, name) {
    try {
        return await flowElement.hasProperty(name) ? await flowElement.getPropertyStringValue(name) : undefined
    } catch (e) {
        return undefined
    }
}

//Converts Excel spreadsheet to separate json objects in a format: {sheet_name: sheet_as_csv_string}
function ExcelToJsObject(excel_location, ignore_hidden_sheets = true, skip_hidden_rows = true, include_blank_rows = false) {
    if (!fs.existsSync(excel_location)) {
        throw Error(`Excel spreadsheet doesn't exist in the specified location "${excel_location}"!`)
    }
    let original = excel.readFile(excel_location)

    let result = {}

    for (let sheet of original.Workbook["Sheets"]) {
        if (ignore_hidden_sheets && sheet["Hidden"]) {
            continue
        }

        result[sheet["name"]] = excel.utils.sheet_to_csv(original.Sheets[sheet["name"]], {blankrows: include_blank_rows, skipHidden: skip_hidden_rows});
    }

    return result
}

//Introduces delay into the process
function Delay(t) {
    return new Promise(function(resolve) {
        setTimeout(function() {
            resolve();
        }, t);
    });
}

module.exports = {
    GetGlobalSwitchConfig,
    GenerateDateString,
    GenerateNewName,
    CreateDataSet,
    DataSetExists,
    GetDataSet,
    GetProperty,
    Delay,
}