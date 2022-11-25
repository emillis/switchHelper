const fs = require("fs");
const path = require("path");
const excel = require("xlsx");
const CsvReadableStream = require("csv-reader");
const createCsvWriter = require('csv-writer').createArrayCsvWriter;

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
async function CreateDataSet(job, datasetName, data, tmp_file_store, datasetModel = "JSON") {
    let allowedDatasetModels = ["JSON", "Opaque"];
    if (!allowedDatasetModels.includes(datasetModel)) {
        throw Error(`Dataset Model "${datasetModel}" is not supported! Allowed dataset models are: "${allowedDatasetModels.join(`", "`)}".`)
    }
    if (!tmp_file_store) {
        let key = `TempMetadataFileLocation`;
        tmp_file_store = GetGlobalSwitchConfig()[key];
        if (!tmp_file_store) {
            throw Error(`Location where to store temporary created files could not been found in the global switch config file where key should be "${key}" for dataset name "${datasetName}"!`)
        }
    }
    //Checking whether the right type of variables are supplied to the function
    if (datasetModel === "JSON" && typeof data !== "object") {
        throw Error(`When using "JSON" DatasetModel, expecting to receive data type "object", got "${typeof data}".`)
    }
    if (datasetModel === "Opaque" && typeof data !== "string") {
        throw Error(`When using "Opaque" DatasetModel, expecting to receive data of type "string", got "${typeof data}".`)
    }
    if (typeof datasetName !== "string" || datasetName === "") {
        throw Error(`Dataset name "${datasetName.toString()}" is invalid!`)
    }
    if (!tmp_file_store || !fs.existsSync(tmp_file_store)) {
        throw Error(`Invalid location "${tmp_file_store}" for storing temporary metadata files!`)
    }

    let location;

    if (datasetModel === "JSON") {
        for (;;) {
            location = path.join(tmp_file_store, `${GenerateNewName("dataset")}.json`);

            if (fs.existsSync(location)) {continue}

            break
        }

        fs.writeFileSync(location, JSON.stringify(data))
    }

    if (datasetModel === "Opaque") {
        location = data
    }

    await job.createDataset(datasetName, location, datasetModel);

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
function ExcelToJsObject(excel_location, options = {}) {
    options = {
        ignore_hidden_sheets: options.ignore_hidden_sheets === undefined ? true : options.ignore_hidden_sheets,
        skip_hidden_rows: options.skip_hidden_rows === undefined ? true : options.skip_hidden_rows,
        include_blank_rows: options.include_blank_rows === undefined ? false : options.include_blank_rows,
        min_no_of_rows: options.min_no_of_rows === undefined ? 2 : options.min_no_of_rows
    };

    if (!fs.existsSync(excel_location)) {
        throw Error(`Excel spreadsheet doesn't exist in the specified location "${excel_location}"!`)
    }
    let original = excel.readFile(excel_location)

    let result = {}

    for (let sheet of original.Workbook["Sheets"]) {
        if (options.ignore_hidden_sheets && sheet["Hidden"]) {
            continue
        }

        const csv = excel.utils.sheet_to_csv(original.Sheets[sheet["name"]], {blankrows: options.include_blank_rows, skipHidden: options.skip_hidden_rows});

        if ((csv.split(/\r\n|\r|\n/).length - 1) < options.min_no_of_rows) {
            continue
        }

        result[sheet["name"]] = csv
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

//CompareString compares two strings with options to make a case-insensitive compare as well as partial match
function CompareStrings(matchToThis, matchThis, options = {}) {
    options.case_sensitive = options.case_sensitive === undefined ? true : options.case_sensitive
    options.match_partial = options.match_partial === undefined ? false : options.match_partial

    if (!options.case_sensitive) {
        matchThis = matchThis.toLowerCase()
        matchToThis = matchToThis.toLowerCase()
    }

    return options.match_partial ? matchToThis.search(matchThis) !== -1 : matchToThis === matchThis;
}

//This function allows to scan a system location and returns the results
//needle - what to look for in the haystack.
//haystack - root system location where to scan
//options - {
//  allowedExt,     //An array of extensions that are allowed to be returned. E.g. ".pdf", ".csv", etc.. If nothing
//                  //is defined, all extensions will be allowed.
//  partialMatch,   //true/false whether to match the name in full or just partially. Default - true
//  returnType,     //Return type can be one of three: "full", "name", "nameProper". Default - "full"
//}
function FindFilesInLocation(needle, haystack, options) {
    options = options || {}
    if (typeof options !== "object") {throw Error(`Options must be of type "object", got "${typeof options}"!`)}
    options = {
        allowedExt: options.allowedExt || [],//empty array if nothing is defined
        allowAllExt: !options.allowedExt || !options.allowedExt.length, //true if nothing is defined
        partialMatch: options.partialMatch === undefined, //true by default
        returnType: options.returnType || "full"
    }
    const allowedReturnTypes = ["full", "name", "nameProper"];
    if (!allowedReturnTypes.includes(options.returnType)) {throw Error(`Wrong returnType entered! Entered: "${options.returnType}", allowed are: "${allowedReturnTypes.join(`", "`)}"`)}

    let results = [];
    const needleTLC = needle.toLowerCase()

    for (let hay of fs.readdirSync(haystack, "utf-8")) {
        if (options.partialMatch && (hay.toLowerCase()).search(needleTLC) === -1) {
            continue
        } else if (!options.partialMatch && hay.toLowerCase() !== needleTLC) {
            continue
        }

        const parsedName = path.parse(hay);

        if (!options.allowedExt.includes(parsedName.ext) && !options.allowAllExt) {
            continue
        }

        if (options.returnType === "full") {
            results.push(path.join(haystack, hay).replaceAll("\\", "/"))
        } else if (options.returnType === "name") {
            results.push(hay)
        } else if (options.returnType === "nameProper") {
            results.push(parsedName.name)
        }
    }

    return results
}

//ParseCsvFile takes in .csv file, places its contents into an object for manipulation and can save it back to .csv
function CsvProcessor(location, options) {
    options = options || {}
    if (!location || !fs.existsSync(location)) {throw Error(`Csv file does not exist in the location "${location}"!`)}
    if (!fs.statSync(location).isFile()) {throw Error(`Location supplied "${location}" is not a file!`)}
    const parsedLocation = path.parse(location);
    if (parsedLocation.ext !== ".csv") {throw Error(`Can only read .csv files, got "${parsedLocation.ext}"`)}

    options = {
        firstRowContainsHeaders: options.firstRowContainsHeaders === undefined ? true : !!options.firstRowContainsHeaders
    }

    let data = {
        headers:[],
        rows:[],
        rowsStartIndex: 0 //Do not delete this.
    }

    //Returns the whole file as js object
    this.getReference = function() {
        return data;
    }

    //Returns headers if there were any
    this.getHeaders = function() {
        return data.headers
    }

    //Returns rows excluding headers
    this.getRows = function() {
        return data.rows
    }

    //Returns an array of values about the headers that match the keyword
    this.findAllHeaders = function(keyword) {
        keyword = keyword.toLowerCase()
        let results = [];

        for (let i = 0; i < data.headers.length; i++) {
            let header = data.headers[i];

            if (header.toLowerCase() !== keyword) {
                continue
            }

            results.push({
                index: i,
                value: header
            })
        }

        return results
    }

    //Returns a line number at which rows start (excluding headers)
    this.rowsStartIndex = function() {
        return data.rowsStartIndex
    }

    //Generates a new .csv file in the location provided.
    //If full path provided (including file name), the csv is saved to that location with that name
    //If only system location is provided (no name), file will be saved there with original name
    //If nothing is provided, original file gets replaced
    this.saveTo = async function(location) {
        if (!location) {location = path.join(parsedLocation.dir, parsedLocation.base)}
        let pLoc = path.parse(location);
        if (!pLoc.ext) {
            location = path.join(location, parsedLocation.base)
            pLoc = path.parse(location)
        }
        if (!fs.existsSync(pLoc.dir)) {fs.mkdirSync(pLoc.dir)}

        const csvWriter = createCsvWriter({path: location});

        const records = [];

        if (data.headers.length > 0) {
            records.push(data.headers)
        }

        if (data.rows.length > 0) {
            records.push(...data.rows)
        }

        return await csvWriter.writeRecords(records)
    }

    let thisFunc = this;
    return new Promise(resolve=>{
        let isHeader = options.firstRowContainsHeaders;
        let rowIndex = 0;

        fs.createReadStream(location, "utf-8")
            .pipe(new CsvReadableStream({ parseNumbers: true, parseBooleans: true, trim: true }))
            .on("data", row=>{
                rowIndex++
                if (isHeader) {
                    data.headers = row
                    isHeader = false
                    return
                }

                if (!data.rowsStartIndex) {
                    data.rowsStartIndex = rowIndex
                }

                data.rows.push(row)
            }).on("end", ()=>{
            resolve(thisFunc)
        })
    })
}

function MatchFilesToCsvData(options = {}) {

}

module.exports = {
    GetGlobalSwitchConfig,
    GenerateDateString,
    GenerateNewName,
    CreateDataSet,
    DataSetExists,
    GetDataSet,
    GetProperty,
    ExcelToJsObject,
    CompareStrings,
    Delay,
    FindFilesInLocation,
    CsvProcessor,
}