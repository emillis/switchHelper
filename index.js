const fs = require("fs");
const path = require("path");
const excel = require("xlsx");
const CsvReadableStream = require("csv-reader");
const createCsvWriter = require('csv-writer').createArrayCsvWriter;

//Constructor for ease of creation of Switch Report
function SwitchReport() {

    let options = {
        PageTitle: "",
        TabTitle: "",
        RowCounts: {
            Error: 0,
            Warning: 0,
            Success: 0,
        },
        MessageByType: {
            Error: [],
            Warning: [],
            Success: [],
            Log: [],
        },
        Rows: [],
    }

    const thisFunction = this;

    this.setPageTitle = function (newTitle) {
        options.PageTitle = newTitle

        return thisFunction;
    }
    this.getPageTitle = function () {
        return options.PageTitle
    }

    this.setTabTitle = function (newTitle) {
        options.TabTitle = newTitle

        return thisFunction;
    }
    this.getTabTitle = function () {
        return options.TabTitle
    }

    this.addErrorRow = function (...messages) {
        thisFunction.addRow("error", ...messages);
        options.MessageByType.Error.push(...messages)
        options.RowCounts.Error++
    }
    this.addWarningRow = function (...messages) {
        thisFunction.addRow("warning", ...messages);
        options.MessageByType.Warning.push(...messages)
        options.RowCounts.Warning++
    }
    this.addSuccessRow = function (...messages) {
        thisFunction.addRow("success", ...messages);
        options.MessageByType.Success.push(...messages)
        options.RowCounts.Success++
    }

    this.addRow = function (rowType, ...messages) {
        let colours = {
            success: "bg-success",
            warning: "bg-warning",
            error: "bg-error",
        }

        let color = colours[rowType] || "bg-default"

        for (let message of messages) {
            options.Rows.push(`
            <div class="row">
              <div class="cell-status ${color}"></div>
              <div class="cell-message">${message}</div>
            </div>
        `)
        }
    }

    this.ErrorCount = function () {
        return options.RowCounts.Error
    }
    this.WarningCount = function () {
        return options.RowCounts.Warning
    }
    this.SuccessCount = function () {
        return options.RowCounts.Success
    }

    this.ListErrors = function () {return options.MessageByType.Error}
    this.ListWarnings = function () {return options.MessageByType.Warning}
    this.ListSuccess = function () {return options.MessageByType.Success}

    this.generateHtmlReport = function () {
        return `
            <!DOCTYPE html>
            <html lang="en">
            <head>
              <meta charset="UTF-8">
              <title>${options.TabTitle}</title>
              <style>
                #page-title {
                  text-align: center;
                }
            
                .row {
                  display: flex;
                  margin: 0 0 .5rem 0;
                }
                .row:hover {
                  background-color: #eee;
                }
                .row .cell-message {
                  flex: 24;
                  padding: 0 0 0 1rem;
                }
                .row .cell-status {
                  flex: 1rem;
                }
            
                #status-info {
                  font-size: .875rem;
                }
                
                .bg-error {
                  background-color: #dc3545;
                }
                .bg-warning {
                  background-color: #ffc107;
                }
                .bg-success {
                  background-color: #28a745;
                }
                .bg-default {
                  background-color: #999;
                }
              </style>
            </head>
            <body>
              <div id="page-title">
                <h2>${options.PageTitle}</h2>
              </div>
              <div id="rows">
                ${options.Rows.join("")}
              </div>
              <hr style="margin: 2rem 0">
              <div id="status-info">
                Time Created: ${GenerateDateString(".", false)}
              </div>
            </body>
            </html>
        `
    }

    this.generateHtmlReportAsFile = function(tmpFileLocation) {
        return CreateNewTmpFile(path.join(tmpFileLocation, GenerateNewName(`tmpHtml`, `_report.html`)), thisFunction.generateHtmlReport())
    }

    this.sendJobToConnection = async function (job, tmpFileLocation, newName) {
        if (!job) {throw `"job" is not provided as an argument to method "sendJobToConnection"!`}
        tmpFileLocation = tmpFileLocation || (GetGlobalSwitchConfig())["TempMetadataFileLocation"]

        const ConnManager = new OutgoingConnectionManager(job, newName);

        const sender = thisFunction.ErrorCount() ? ConnManager.error : thisFunction.WarningCount() ? ConnManager.warning : ConnManager.success

        await sender(thisFunction.generateHtmlReportAsFile(tmpFileLocation));
    }
}

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

    try {
        return JSON.parse(fs.readFileSync(loc, "utf-8"))
    } catch (e) {
        throw `Invalid JSON file format referred from "${env_var}" environmental variable, location "${loc}"! Original error: "${e.toString()}"`
    }
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

//Creates new temporary file. If nothing is provided, new random file is created at a location defined in the
//global switch config file. Alternatively, you can provide either folder path only, file name only or full path.
//After the file was created, full path to it is returned
function CreateNewTmpFile(location = "", content = "", overwrite = false) {
    if (typeof location !== "string") {throw Error(`Invalid location provided. Expected "string", got ${typeof location}`)}
    if (typeof content !== "string") {throw Error(`Wrong content type provided! Expected "string", got ${typeof content}`)}
    let parsedLoc = path.parse(location)
    if (parsedLoc.dir === "") {
        parsedLoc = path.parse(path.join(GetGlobalSwitchConfig()["TempMetadataFileLocation"], parsedLoc.base))
    }
    if (parsedLoc.ext === "") {
        parsedLoc = path.parse(path.join(parsedLoc.dir, parsedLoc.base, GenerateNewName("tmp_generic_file", ".txt")))
    }

    if (!fs.existsSync(parsedLoc.dir)) {fs.mkdirSync(parsedLoc.dir)}

    let fullPath = path.join(parsedLoc.dir, parsedLoc.base);

    for (;;) {
        if (fs.existsSync(fullPath) && !overwrite) {
            fullPath = path.join(parsedLoc.dir, `${GenerateDateString()}-${parsedLoc.base}`)
            continue
        }

        break
    }

    fs.writeFileSync(fullPath, content, "utf-8")

    return fullPath
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
        min_no_of_rows: options.min_no_of_rows === undefined ? 1 : options.min_no_of_rows
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

    return options.match_partial ? matchToThis.includes(matchThis) : matchToThis === matchThis;
}

//This function allows to scan a system location and returns the results
//needle - what to look for in the haystack.
//haystack - root system location where to scan
//options - {
//  allowedExt,             //An array of extensions that are allowed to be returned. E.g. ".pdf", ".csv", etc.. If nothing
//                          //is defined, all extensions will be allowed.
//  partialMatch,           //true/false whether to match the name in full or just partially. Default - true
//  caseSensitive,          //true/false whether matching is going to be case-sensitive
//  returnType,             //Return types, needs to be an array with any number of following values: "full", "name", "nameProper". Default - ["full"]
//  depth,                  //Folder hierarchy scan depth
//  lookFor,                //What to look for. Allowed options are: "files", "folders", "both". Default - "files"
//  ifHaystackNotFound,     //What to do if haystack location doesn't exist. Options: "returnEmptyResults", "throwError". Default = "returnEmptyResults"
//}
async function FindInLocation(needle, haystack, options) {
    const startedTime = Date.now();
    options = options || {}
    if (typeof options !== "object") {throw Error(`Options must be of type "object", got "${typeof options}"!`)}
    options = {
        allowedExt: options.allowedExt || [],//empty array if nothing is defined
        allowAllExt: true, //true if nothing is defined
        partialMatch: options.partialMatch === undefined ? true : options.partialMatch, //true by default
        caseSensitive: !!options.caseSensitive,
        returnType: options.returnType && options.returnType.length ? options.returnType : ["full"],
        depth: options.depth || 0,
        lookFor: options.lookFor === undefined ? "files" : `${options.lookFor}`,
        ifHaystackNotFound: options.ifHaystackNotFound || "returnEmptyResults"
    }
    let response = {results: {}, stats: {foldersScanned: 0, entitiesTested: 0, timeTaken: 0, resultsFound: 0}}

    needle = options.caseSensitive ? needle : needle.toLowerCase()

    let parsedAllowedExtensions = [];
    for (let ext of options.allowedExt) {
        ext = `${ext}`.replaceAll(` `, ``).toLowerCase()

        if (!ext) {continue}

        //Adding a dot (.) at the beginning if one doesn't exist
        ext[0] === `.` || (ext = `.${ext}`)

        if (parsedAllowedExtensions.includes(ext)) {
            continue
        }

        options.allowAllExt = false
        parsedAllowedExtensions.push(ext)
    }
    options.allowedExt = parsedAllowedExtensions;

    const allowedReturnTypes = {full: "full", name: "name", nameProper: "nameProper"};
    let allowedLookFor = {files: "files", folders: "folders", both: "both"};
    let allowedIfHaystackNotFound = {returnEmptyResults: "returnEmptyResults", throwError: "throwError"};
    if (!allowedIfHaystackNotFound[options.ifHaystackNotFound]) {throw `Option "${options.ifHaystackNotFound}" not allowed in field "ifHaystackNotFound". Allowed options are: "${Object.keys(allowedIfHaystackNotFound).join(`", "`)}"`}
    if (typeof options.returnType !== "object") {throw `Option "returnType" must be an array! Got "${typeof options.returnType}"!`}
    for (const returnType of options.returnType) {if (!Object.values(allowedReturnTypes).includes(returnType)) {throw `Wrong returnType entered! Entered: "${options.returnType}", allowed are: "${Object.values(allowedReturnTypes).join(`", "`)}"`}}
    if (!Object.values(allowedLookFor).includes(options.lookFor)) {throw `Value "${options.lookFor}" passed to option "lookFor" is invalid! Allowed values are: "${Object.values(allowedLookFor).join(`", "`)}"`}
    if (!fs.existsSync(haystack)) {
        if (options.ifHaystackNotFound === allowedIfHaystackNotFound.throwError) {
            throw `Haystack "${haystack}" provided does not exist!`
        }

        if (options.ifHaystackNotFound === allowedIfHaystackNotFound.returnEmptyResults) {
            return response
        }
    }

    async function scanFolder(haystack, needle, depth) {
        let newDepth = depth - 1;
        response.stats.foldersScanned++

        for (let dirent of fs.readdirSync(haystack, {withFileTypes: true, encoding: "utf-8"})) {
            const hayOriginal = dirent.name;
            response.stats.entitiesTested++
            const hay = options.caseSensitive ? hayOriginal : hayOriginal.toLowerCase()
            const fullPath = path.join(haystack, hayOriginal).replaceAll("\\", "/");

            if (dirent.isDirectory() && newDepth >= 0) {
                scanFolder(fullPath, needle, newDepth)
            }

            if (options.partialMatch && hay.search(needle) === -1) {
                continue
            } else if (!options.partialMatch && hay !== needle) {
                continue
            }

            const parsedName = path.parse(hayOriginal);

            if (!options.allowAllExt && !options.allowedExt.includes(parsedName.ext.toLowerCase())) {
                continue
            }

            if (options.lookFor !== allowedLookFor.both) {
                if (options.lookFor === allowedLookFor.folders && dirent.isFile()) {
                    continue
                }

                if (options.lookFor === allowedLookFor.files && dirent.isDirectory()) {
                    continue
                }
            }

            for (const returnType of options.returnType || []) {
                response.results[returnType] = response.results[returnType] || []
                response.results[returnType].push(returnType === allowedReturnTypes.full ? fullPath : returnType === allowedReturnTypes.name ? hayOriginal : returnType === allowedReturnTypes.nameProper ? parsedName.name : undefined)
            }
            response.stats.resultsFound++
        }
    }

    if (!fs.existsSync(haystack)) {
        return
    }

    await scanFolder(haystack, needle, options.depth)

    response.stats.timeTaken = Date.now() - startedTime

    return response
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

//MatchFilesToCsvData allows to match a column from .csv file to files in a location and populate another column with the results
//Available options are as follows:
//  csvLocation: Full path to a .csv file
//  saveLocation: Location where you'd wish to save the result. If none is provided, the file gets overwritten
//  matching: [ //This allows matching any number of column to external files
//      {
//         columnToMatch: Column title that suppose to match with the files from "scanLocation" option
//         columnForResults: Column title where the matched results should be placed
//         matchedMethod: "full"|"partial". Method of matching .csv column to files. "full" - must match the full name. "partial" - can
//                     match only part of the name.
//         resultsAppendMethod: "full"|"name"|"nameProper". What will be placed in the "columnForResults". "full" - will append
//                           full system path to the file. "name" - will append only the name. "nameProper" - will append
//                           name without the extension.
//         scanLocation: Location where to look for the files.
//         useDifferentRootLocation: If "resultsAppendMethod" is set to "full" this allows to replace the root location
//         ifColumnToMatchNotPresent: Allowed three options: "success", "warning", "error". Will place a log message to report accordingly
//      }
//  ]
async function MatchFilesToCsvData(options = {}) {
    const reporter = new SwitchReport();

    options = {
        csvLocation: options.csvLocation,
        saveLocation: options.saveLocation || options.csvLocation,
        matching: options.matching || []
    }

    const allowedMatchMethods = ["full", "partial"];
    const allowedResultsAppendMethod = ["full", "name", "nameProper"];
    const allowedIfColumnToMatchNotPresent = ["success", "warning", "error"];

    if (!fs.existsSync(options.csvLocation)) {throw Error(`Csv file does not exist in the location "${options.csvLocation}"`)}
    if (path.parse(options.csvLocation).ext !== ".csv") {throw Error(`File in location "${options.csvLocation}" is not a .csv file!`)}

    for (let match of options.matching) {
        match.columnToMatch = (match.columnToMatch||"").toString()
        match.columnForResults = (match.columnForResults || "FileMatchResults").toString()
        match.matchMethod = match.matchMethod === undefined ? "full" : match.matchMethod
        match.resultsAppendMethod = match.resultsAppendMethod || ""
        match.scanLocation = match.scanLocation || ""
        match.useDifferentRootLocation = match.useDifferentRootLocation || ""
        match.ifColumnToMatchNotPresent = match.ifColumnToMatchNotPresent || "success"

        if (!match.columnToMatch) {throw Error(`Column to match in the .csv file is note defined! Expected a string, got "${match.columnToMatch}"`)}
        if (!match.columnForResults) {throw Error(`Column name where to put the results was not defined! Expected a string value, got "${match.columnForResults}"`)}
        if (!match.scanLocation) {throw Error(`Scan location was not provided! Excepted system location, got "${match.scanLocation}"`)}
        if (!fs.existsSync(match.scanLocation)) {throw Error(`Scan location "${match.scanLocation}" does not exist!`)}
        if (path.parse(match.scanLocation).ext) {throw Error(`File location provided instead of a system path. Expected system path, got path to file "${match.scanLocation}"`)}
        if (!allowedMatchMethods.includes(match.matchMethod)) {throw Error(`Match method "${match.matchMethod}" is not allowed! Allowed match methods are: "${allowedMatchMethods.join(`", "`)}"`)}
        if (!allowedResultsAppendMethod.includes(match.resultsAppendMethod)) {throw Error(`Results append method "${match.resultsAppendMethod}" is not allowed! Allowed methods are: "${allowedResultsAppendMethod.join(`", "`)}"`)}
        if (!allowedIfColumnToMatchNotPresent.includes(match.ifColumnToMatchNotPresent)) {throw Error(`Option "ifColumnToMatchNotPresent" is allowed to have values "${allowedIfColumnToMatchNotPresent.join(`" or "`)}", got "${match.ifColumnToMatchNotPresent}"`)}
    }

    let csvFile = await new CsvProcessor(options.csvLocation);
    let csvFileData = csvFile.getReference();

    for (let match of options.matching) {
        let columnToMatch = csvFile.findAllHeaders(match.columnToMatch);
        let columnsForResults = csvFile.findAllHeaders(match.columnForResults);

        if (columnToMatch.length < 1) {
            const message = `There were no column "<b>${match.columnToMatch}</b>" present, therefore, no file matching has been made.`;

            if (match.ifColumnToMatchNotPresent === "success") {
                reporter.addSuccessRow(message)
            } else if (match.ifColumnToMatchNotPresent === "warning") {
                reporter.addWarningRow(message)
            } else {
                reporter.addErrorRow(message)
            }

            continue
        }
        if (columnToMatch.length > 1) {
            reporter.addErrorRow(`Csv file cannot contain more than one column with title "<b>${match.columnToMatch}</b>". Found <b>${columnToMatch.length}</b> columns!`)
            continue
        }

        columnToMatch = columnToMatch[0]

        if (columnsForResults < 1) {
            csvFileData.headers.push(match.columnForResults);
            columnsForResults = csvFile.findAllHeaders(match.columnForResults)
        }

        let o = {
            allowedExt: [".pdf"],
            partialMatch: match.matchMethod === undefined ? "partial" : match.matchMethod,
            returnType: match.resultsAppendMethod === undefined ? "full" : match.resultsAppendMethod,
        }

        for (let i = 0; i< csvFileData.rows.length; i++) {
            let row = csvFileData.rows[i];
            const toMatch = row[columnToMatch.index]
            const foundFiles = FindInLocation(toMatch, match.scanLocation, o)
            const rowIndex = i + csvFile.rowsStartIndex();

            if (foundFiles.length < 1) {
                reporter.addWarningRow(`Could not find a match for value "<b>${toMatch}</b>" (column "<b>${columnToMatch.value}</b>", row "<b>${rowIndex}</b>")!`)
                continue
            }

            if (foundFiles.length > 1) {
                let fileNames = [];

                for (let f of foundFiles) {fileNames.push(path.parse(f).base)}

                reporter.addWarningRow(`Value "<b>${toMatch}</b>" (column "<b>${columnToMatch.value}</b>", row "<b>${rowIndex}</b>") have matched multiple (<b>${foundFiles.length}</b>) files! Those files are: "<b>${fileNames.join(`</b>", "<b>`)}</b>". Only one file is allowed to be matched!`)
                continue
            }

            const foundFile = match.resultsAppendMethod === "full" && match.useDifferentRootLocation ? path.join(match.useDifferentRootLocation, path.parse(foundFiles[0]).base) : foundFiles[0]

            for (let resultColumn of columnsForResults) {
                csvFileData.rows[i][resultColumn.index] = foundFile;
            }

            reporter.addSuccessRow(`Column "<b>${columnToMatch.value}</b>", row "<b>${rowIndex}</b>", value "<b>${toMatch}</b>" matched file "<b>${path.parse(foundFile).base}</b>". Placing the result to ${columnsForResults.length} ${columnsForResults.length > 1 ? "columns" : "column"} in the .csv file, named "<b>${columnsForResults[0].value}</b>".`);
        }
    }

    return {
        report: reporter,

        //Saves file to supplied location. If location is not supplied, options.saveLocation is used
        saveFile: async function (loc = options.saveLocation) {
            await csvFile.saveTo(loc)
            return `Saved to location "${loc}".`
        },
    }
}

//Provides an easy way of managing "Traffic Lights" switch connection type
function OutgoingConnectionManager(switchJob, newName) {
    const job = switchJob;
    const allowedLevels = ["success", "warning", "error"];

    async function send(level, report) {
        if (!allowedLevels.includes(level)) {throw Error(`Invalid connection level supplied! Expected "${allowedLevels.join(`" or "`)}", got "${level}"`)}
        if (report && !fs.existsSync(report.toString())) {throw Error(`Report doesn't exist in the location "${report.toString()}" provided!`)}

        try {
            let reportName = undefined;
            let outputFileName = undefined;
            if (newName) {
                const newNameNameOnly = path.parse(newName).name;

                reportName = `${newNameNameOnly}${path.parse(report).ext}`;
                outputFileName = `${newNameNameOnly}${path.parse(await job.getName()).ext}`;
            }

            if (report) {
                const reportJob = await job.createChild(report);
                await reportJob.sendToLog(level, "Opaque", reportName);
            }

            await job.sendToData(level, outputFileName);
        } catch (e) {
            await job.log("error", e.toString());
        }
    }

    this.success = async function (report){
        await send("success", report);
    }

    this.warning = async function (report){
        await send("warning", report);
    }

    this.error = async function (report){
        await send("error", report);
    }
}

module.exports = {
    SwitchReport,
    GetGlobalSwitchConfig,
    GenerateDateString,
    GenerateNewName,
    CreateNewTmpFile,
    CreateDataSet,
    DataSetExists,
    GetDataSet,
    GetProperty,
    ExcelToJsObject,
    CompareStrings,
    Delay,
    FindInLocation,
    CsvProcessor,
    MatchFilesToCsvData,
    OutgoingConnectionManager,
}