var XlsxStreamReader = require("xlsx-stream-reader");
const path = require("path");
const fs = require("fs");
const xlsx = require("xlsx");

async function processAndSaveData(filePath) {
    if (!fs.existsSync(filePath)) {
        console.log("File not exists:", filePath);
        return;
    }

    // Delete all files from the output folder before processing
    clearOutputFolder();

    const chunkSize = 5000;
    let rowCount = 0;
    let skippedRows = 0;
    let currentChunk = [];
    let chunkNumber = 1;
    let headerRow = null;

    const reader = new XlsxStreamReader();

    reader.on("worksheet", (workSheetReader) => {
        console.log(`Processing: ${filePath}`);

        if (workSheetReader.id > 1) {
            workSheetReader.skip();
            return;
        }

        workSheetReader.on("row", (row) => {
            if (row.attributes.r == 1 && !headerRow) {
                // Capture the first non-empty row as the header
                const hasData = row.values.some((rowVal) => !!rowVal);
                if (hasData) {
                    headerRow = row.values;
                }
            } else {
                const hasData = row.values.some((rowVal) => !!rowVal);

                if (hasData) {
                    rowCount++;
                    currentChunk.push(row);

                    if (rowCount % chunkSize === 0) {
                        saveChunkToFile(currentChunk, headerRow, chunkNumber++);
                        currentChunk = [];
                    }
                } else {
                    skippedRows++;
                }
            }
        });

        workSheetReader.on("end", async () => {
            if (currentChunk.length > 0) {
                saveChunkToFile(currentChunk, headerRow, chunkNumber++);
            }

            console.log("Total chunks:", chunkNumber - 1);
            console.log("Total rows:", rowCount);
            console.log(`Skipped ${skippedRows} rows`);

            currentChunk.length = 0;
        });

        workSheetReader.process();
    });

    fs.createReadStream(filePath).pipe(reader);
}

function saveChunkToFile(chunk, headerRow, chunkNumber) {
    const timestamp = new Date().toISOString().replace(/:/g, '-');
    const fileName = `part${chunkNumber}_${timestamp}.xlsx`;
    const outputPath = path.join("output", fileName);

    const sheetData = chunk.map(row => mapRowToHeader(headerRow, row.values));
    const sheet = xlsx.utils.json_to_sheet(sheetData);
    const workbook = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(workbook, sheet, 'Sheet1');
    xlsx.writeFile(workbook, outputPath);

    console.log(`Saved chunk to file: ${fileName}`);
    console.log("Rows in this chunk:");
    sheetData.forEach((row, index) => {
        console.log(`Row ${index + 1}:`, row);
    });
}

function clearOutputFolder() {
    const outputFolderPath = path.join(__dirname, 'output');

    if (fs.existsSync(outputFolderPath)) {
        const files = fs.readdirSync(outputFolderPath);

        for (const file of files) {
            const filePath = path.join(outputFolderPath, file);
            fs.unlinkSync(filePath);
            console.log(`Deleted file: ${filePath}`);
        }

        console.log("Output folder cleared.");
    } else {
        console.log("Output folder does not exist.");
    }
}

function mapRowToHeader(headerRow, rowValues) {
    const mappedRow = {};
    headerRow.forEach((field, index) => {
        mappedRow[field] = rowValues[index];
    });
    return mappedRow;
}

// Example usage with command line argument:
const filePath = process.argv[2];
processAndSaveData(filePath);
