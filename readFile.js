const parquet = require("parquetjs-lite");
const cheerio = require("cheerio");
const axios = require("axios");
const XLSX = require("xlsx");

async function readParquetFile(path) {
  const reader = await parquet.ParquetReader.openFile(path);
  const cursor = reader.getCursor();
  const records = [];

  let record = null;
  while ((record = await cursor.next())) {
    records.push(record);
  }

  await reader.close();
  return records;
}

async function scrapeAddresses(url) {
  try {
    const response = await axios.get(url);
    const $ = cheerio.load(response.data);

    const patterns = [
      "address",
      ".address",
      "#address",
      ".footer .contact-info .address",
      ".contact .address",
      ".location",
      ".contact-address",
      ".contact-info p",
      ".footer-address",
      ".footer-info .address",
      ".company-address",
      ".main-address",
      ".business-info .address",
      ".address-info",
      ".office-location",
      ".address-block",
      ".contact-details .address",
      ".address-container",
      ".street-address",
      ".postal-address",
      ".mailing-address",
      ".location-info .address",
      ".contact-address .street",
      ".address-section .location",
      ".business .address",
      ".company .address",
      ".company-address .street",
      ".business-address",
      ".company-info .address",
      ".main-footer .address",
      ".store-location",
      ".store-info .address",
      ".location .street",
    ];

    let address = "";

    for (const pattern of patterns) {
      const elements = $(pattern);
      if (elements.length > 0) {
        elements.each((index, element) => {
          address += $(element).text().trim() + "\n";
        });
        break;
      }
    }

    if (!address.trim()) {
      console.error("No address found on", url);
    }

    return address.trim();
  } catch (error) {
    console.error("Error scraping address from HTML", url, ":", error.message);
    throw error;
  }
}

function writeExcelFile(data, outputPath) {
  const validData = data.filter((record) => record.Address !== "");
  const ws = XLSX.utils.json_to_sheet(validData);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Addresses");
  XLSX.writeFile(wb, outputPath);
}

async function main(parquetFilePath, excelOutputPath) {
  try {
    const records = await readParquetFile(parquetFilePath);
    const data = [];

    const scrapePromises = records.map(async (record) => {
      const url = "https://" + record.domain;
      try {
        const address = await scrapeAddresses(url);
        data.push({ URL: url, Address: address });
        console.log("Address scraped for", url);
      } catch (error) {
        console.error(
          "Error scraping address for URL:",
          url,
          ":",
          error.message
        );
      }
    });

    await Promise.all(scrapePromises);

    writeExcelFile(data, excelOutputPath);
    console.log("Excel file written successfully");
  } catch (error) {
    console.error("Error:", error.message);
  }
}

main("file.parquet", "output.xlsx");
