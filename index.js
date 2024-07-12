const { Pool } = require("pg");
const axios = require("axios");
const OpenAI = require("openai");
require("dotenv").config();

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: 5432,
});

const languages = [
  "English",
  "Turkish",
  "Arabic",
  "Spanish",
  "Portuguese",
  "Russian",
  "French",
  "German",
  "Chinese",
  "Japanese",
  "Italian",
  // "Polish",
  // "Norwegian",
  // "Vietnamese",
  // "Malay",
  // "Ozbek",
  "Korean",
]; // Add more languages as needed
const batchSize = 10;

async function translateWords(words) {
  const prompt = `Translate the following words related to sports betting into the following languages: ${languages.join(
    ", "
  )}. 
  Provide the translations in a JSON object where the keys are the language names and the values are arrays of translations.
  Words: ${words.join(", ")}`;

  try {
    const response = await openai.chat.completions.create({
      model: "gpt-3.5-turbo",
      messages: [
        { role: "system", content: "You are a helpful assistant." },
        { role: "user", content: prompt },
      ],
      max_tokens: 4096,
    });
    const responseString = response.choices[0].message.content.trim();
    const jsonPattern = /{.*}/s;
    const match = responseString.match(jsonPattern);
    let extractedJsonString = match[0];
    extractedJsonString = extractedJsonString.replace(/،/g, ",");
    console.log(extractedJsonString);
    const translations = JSON.parse(extractedJsonString);
    return translations;
  } catch (error) {
    console.error("An error occurred during translation:", error.message);
    return null;
  }
}

async function makeMarketConstants() {
  const client = await pool.connect();
  try {
    const removeMarketsQuery = `DELETE FROM market_constants WHERE data_feed='huge_data'`;
    await client.query(removeMarketsQuery);
    const removeOutcomeQuery = `DELETE FROM outcome_constants WHERE data_feed='huge_data'`;
    await client.query(removeOutcomeQuery);

    // Get the maximum id from the market_constants table
    const maxMarketIdQuery = `
      SELECT MAX(id) AS max_id FROM market_constants
    `;
    const maxMarketIdResult = await client.query(maxMarketIdQuery);
    const maxMarketId = maxMarketIdResult.rows[0].max_id || 0; // If no records exist, set maxId to 0

    
    const maxOutcomeIdQuery = `
      SELECT MAX(id) AS max_id FROM outcome_constants
    `;
    const maxOutcomeIdResult = await client.query(maxOutcomeIdQuery);
    const maxOutcomeId = maxOutcomeIdResult.rows[0].max_id || 0; // If no records exist, set maxId to 0

    const response = await axios.get(
      "https://demofeed.betapi.win/FeedApi/market-definitions"
    );
    const markets = response.data.data;
    console.log("MARKETS:", markets.length);
    let marketId = parseInt(maxMarketId) + 1;
    let outcomeId = parseInt(maxOutcomeId) + 1;
    for (const market of markets) {
      for (const market_info of market.market_templates) {
        const insertQuery = `
            INSERT INTO market_constants 
              (id, reference_id, description, groups, lo_id, lco_id, valid_specifier_value, specifiers, sports, "order", is_translated, data_feed) 
            VALUES 
              ($1, $2, $3, null, null, null, null, null, $4, $5, $6, $7)
          `;
        await client.query(insertQuery, [
          marketId,
          market_info.id,
          market_info.name,
          "",
          marketId,
          false,
          "huge_data",
        ]);
        marketId++;
        for (const outcome of market_info.outcomes) {
          const insertQuery = `
            INSERT INTO outcome_constants 
              (id, reference_id, name, "order", is_translated, data_feed) 
            VALUES 
              ($1, $2, $3, $4, $5, $6)
          `;
          await client.query(insertQuery, [
            outcomeId,
            outcome.id,
            outcome.name,
            outcomeId,
            false,
            "huge_data",
          ]);
          outcomeId++;
        }
      }
    }
  } catch (error) {
    console.error(
      "Error inserting into/updating market_constants:",
      error.message
    );
  } finally {
    client.release();
  }
}

async function makeTournaments() {
  const client = await pool.connect();
  try {
    const removeQuery = `DELETE FROM tournaments WHERE data_feed='huge_data'`;
    await client.query(removeQuery);
    const maxTournamentIdQuery = `
      SELECT MAX(id) AS max_id FROM tournaments
    `;
    const maxTournamentIdResult = await client.query(maxTournamentIdQuery);
    const maxTournamentId = maxTournamentIdResult.rows[0].max_id || 0; // If no records exist, set maxId to 0

    let tournamentId = parseInt(maxTournamentId) + 1;

    const sportsQuery = `SELECT * FROM sports WHERE data_feed='huge_data'`
    const sports = await client.query(sportsQuery)

    const countriesQuery = `SELECT * FROM countries WHERE data_feed='huge_data'`
    const countries = await client.query(countriesQuery)

    for (const sport of sports.rows) {
      for (const country of countries.rows) {
        console.log(`Sport: ${sport.reference_id}, Country: ${country.reference_id}`);
        // console.log("RESPONSE:",  `https://demofeed.betapi.win/FeedApi/tournaments?sport_id=${sport.reference_id}&country_id=${country.reference_id}`)
        const response = await axios.get(
          `https://demofeed.betapi.win/FeedApi/tournaments?sport_id=${sport.reference_id}&country_id=${country.reference_id}`
        );
        console.log("RESPONSE:", response.data)
        const tournaments = response.data.data;
        console.log("TOURNAMENTS: ", tournaments);
        if (!tournaments) continue;
        for (const tournament of tournaments) {
          const dataFeed = "huge_data";
          const insertQuery = `
            INSERT INTO tournaments (id, reference_id, sport_id, country_id, name, abbr, created_at, updated_at, deleted_at, "order", is_translated, flag, data_feed)
            VALUES ($1, $2, $3, $4, $5, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, $6, $7, $8, $9)
          `;
          await client.query(insertQuery, [
            tournamentId,
            tournament.id,
            tournament.sport_country.sport_id,
            tournament.sport_country.country_id,
            tournament.name,
            tournamentId,
            false,
            "",
            dataFeed,
          ]);
          tournamentId++;
        }
      }
    }
  } catch (error) {
    console.error("An error occurred during making tournaments:", error.message);
  } finally {
    client.release();
  }
}

async function makeCountries() {
  const client = await pool.connect();
  try {
    const removeQuery = `DELETE FROM countries WHERE data_feed='huge_data'`;
    await client.query(removeQuery);

    const maxCountryIdQuery = `
      SELECT MAX(id) AS max_id FROM countries
    `;
    const maxCountryIdResult = await client.query(maxCountryIdQuery);
    const maxCountryId = maxCountryIdResult.rows[0].max_id || 0; // If no records exist, set maxId to 0

    let countryId = parseInt(maxCountryId) + 1;

    const response = await axios.get(
      "https://demofeed.betapi.win/FeedApi/countries"
    );
    const countries = response.data.data;
    console.log("COUNTRIES: ", countries.length);
    for (const country of countries) {
      const dataFeed = "huge_data";
      const insertQuery = `
        INSERT INTO countries (id, name, abbr, created_at, updated_at, deleted_at, "order", reference_id, is_translated, flag, data_feed)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, $4, $5, $6, $7, $8)
      `;
      await client.query(insertQuery, [
        countryId,
        country.name,
        country.iso2,
        countryId,
        country.id,
        false,
        "",
        dataFeed,
      ]);
      countryId++;
    }
  } catch (error) {
    console.error("Error inserting into market_constants:", error.message);
  } finally {
    client.release();
  }
}

async function makeSports() {
  const client = await pool.connect();
  try {
    const removeQuery = `DELETE FROM sports WHERE data_feed='huge_data'`;
    await client.query(removeQuery);

    const maxSportIdQuery = `
      SELECT MAX(id) AS max_id FROM sports
    `;
    const maxSportIdResult = await client.query(maxSportIdQuery);
    const maxSportId = maxSportIdResult.rows[0].max_id || 0; // If no records exist, set maxId to 0

    let sportId = parseInt(maxSportId) + 1;

    const response = await axios.get(
      "https://demofeed.betapi.win/FeedApi/sports"
    );
    const sports = response.data.data;
    console.log("SPORTS:", sports.length);
    for (const sport of sports) {
      slug = sport.name;
      slug = slug.toLowerCase();
      slug = slug.replace(" ", "_");
      if (sport.id) {
        const query = `
          INSERT INTO sports 
            (id, name, type, created_at, updated_at, deleted_at, slug, reference_id, "order", status, is_translated, flag, data_feed) 
          VALUES 
            ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, $4, $5, $6, $7, $8, $9, $10)
        `;
        const values = [
          sportId,
          sport.name,
          "",
          slug,
          sport.id,
          sportId,
          true,
          false,
          "",
          "huge_data",
        ];
        await client.query(query, values);
        sportId++;
      }
    }
  } catch (error) {
    console.error("Error inserting into sports:", error.message);
  } finally {
    client.release();
  }
}

async function makeSportGroup() {
  const client = await pool.connect();
  try {
    let queryText = `SELECT * FROM sports order by id`;
    const sports = await client.query(queryText);
    queryText = `SELECT * FROM market_constants`;
    const markets = await client.query(queryText);

    const maxIdQuery = `
      SELECT MAX(id) AS max_id FROM sport_market_groups
    `;
    const maxIdResult = await client.query(maxIdQuery);
    const maxId = maxIdResult.rows[0].max_id || 0; // If no records exist, set maxId to 0
    let id = parseInt(maxId) + 1;
    for (const sport of sports.rows) {
      for (const market of markets.rows) {
        let groups = market.groups;
        if (groups == null) groups = "all";
        let groupList = groups.split("|");
        const sportId = sport.id;
        const marketId = market.id;
        const sportName = sport.slug;
        const marketName = market.description;
        const marketSports = market.sports.split(",");
        if (marketSports.indexOf(sportName) === -1) continue;
        const checkQuery = `SELECT * FROM sport_market_groups WHERE sport_id = ${sportId} AND market_id = ${marketId}`;
        const checkResult = await client.query(checkQuery);
        if (checkResult.rows.length > 0) continue;
        if (groupList.length == 1 && groupList[0] === "all") {
          console.log(id);
          const query = `
            INSERT INTO sport_market_groups 
              (id, sport_id, market_id, group_id, sport_name, group_name, market_name, created_at, updated_at) 
            VALUES 
              ($1, $2, $3, null, $4, null, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `;
          const values = [id, sportId, marketId, sportName, marketName];

          await client.query(query, values);
          id++;
          console.log(query);
          // await client.query(query);
        }
        for (const group of groupList) {
          console.log(id);
          if (group === "all") continue;
          let query = `SELECT * FROM market_groups WHERE market_group = '${group}'`;
          const result = await client.query(query);
          const groupId = result.rows[0].id;
          const groupName = result.rows[0].market_group;
          query = `INSERT INTO sport_market_groups (id, sport_id, market_id, group_id, sport_name, group_name, market_name, created_at, updated_at) VALUES ('${id}', '${sportId}', '${marketId}', '${groupId}', '${sportName}', '${groupName}', '${marketName}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`;
          id++;
          await client.query(query);
        }
      }
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function makeDictionaries() {
  const client = await pool.connect();
  try {
    // const deleteDictionaryQuery = `Delete from dictionaries`;
    // const deleteTranslationQuery = `Delete from translations`;
    // await client.query(deleteDictionaryQuery);
    // await client.query(deleteTranslationQuery);

    // const rollbackThemesQuery = `UPDATE theme_dictionaries SET is_translated = false`
    // const rollbackSportsQuery = `UPDATE sports SET is_translated = false`
    // const rollbackCountriesQuery = `UPDATE countries SET is_translated = false`
    // const rollbackTournamentsQuery = `UPDATE tournaments SET is_translated = false`
    // const rollbackCompetitorsQuery = `UPDATE competitors SET is_translated = false`
    // const rollbackMarketsQuery = `UPDATE market_constants SET is_translated = false`
    // const rollbackOutcomesQuery = `UPDATE outcome_constants SET is_translated = false`
    // await client.query(rollbackThemesQuery);
    // await client.query(rollbackSportsQuery);
    // await client.query(rollbackCountriesQuery);
    // await client.query(rollbackTournamentsQuery);
    // await client.query(rollbackCompetitorsQuery);
    // await client.query(rollbackMarketsQuery);
    // await client.query(rollbackOutcomesQuery);

    await makeDictionariesThemes();
    await makeDictionariesSports();
    await makeDictionariesCountries();
    await makeDictionariesTournaments();
    await makeDictionariesCompetitors();
    await makeDictionariesMarkets();
    await makeDictionariesOutcomes();
  } catch (error) {
    console.error("Error makeDictionaries", error.message);
  } finally {
    client.release();
  }
}

async function processBatch(rows, type) {
  let words;
  if (type == "themes") {
    words = rows.map((row) => row.key);
  } else if (type == "sports") {
    words = rows.map((row) => row.name);
  } else if (type == "countries") {
    words = rows.map((row) => row.name);
  } else if (type == "tournaments") {
    words = rows.map((row) => row.name);
  } else if (type == "competitors") {
    words = rows.map((row) => row.name);
  } else if (type == "markets") {
    words = rows.map((row) => row.description);
  } else if (type == "outcomes") {
    words = rows.map((row) => row.name);
  }
  // const words = rows.map((row) => row.key); // Assuming 'key' is the column containing the words
  const translations = await translateWords(words);

  if (!translations) {
    // throw new Error("Failed to get translations");
    console.log("Failed to get translations");
    return;
  }

  console.log(translations);

  const client = await pool.connect();
  try {
    let index = 0;
    for (const row of rows) {
      const dictionaryId = row.dictionaryId;
      for (const language of languages) {
        let shortLanguage = "";
        if (language == "English") shortLanguage = "en";
        else if (language == "Turkish") shortLanguage = "tr";
        else if (language == "Arabic") shortLanguage = "ar";
        else if (language == "Spanish") shortLanguage = "es";
        else if (language == "Portuguese") shortLanguage = "pt";
        else if (language == "Russian") shortLanguage = "ru";
        else if (language == "French") shortLanguage = "fr";
        else if (language == "German") shortLanguage = "de";
        else if (language == "Chinese") shortLanguage = "zh";
        else if (language == "Japanese") shortLanguage = "ja";
        else if (language == "Italian") shortLanguage = "it";
        // else if (language == "Polish") shortLanguage = "pl";
        // else if (language == "Norwegian") shortLanguage = "no";
        // else if (language == "Vietnamese") shortLanguage = "vi";
        // else if (language == "Malay") shortLanguage = "ma";
        // else if (language == "Ozbek") shortLanguage = "oz";
        else if (language == "Korean") shortLanguage = "ko";

        const query = `INSERT INTO translations (dictionary_id, language, value, created_at, updated_at, deleted_at) 
                         VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)`;
        await client.query(query, [
          dictionaryId,
          shortLanguage,
          translations[language][index],
        ]);

        let updateQuery = "";
        if (type == "themes") {
          updateQuery = `UPDATE theme_dictionaries SET is_translated = true WHERE id = $1`;
        } else if (type == "sports") {
          updateQuery = `UPDATE sports SET is_translated = true WHERE id = $1`;
        } else if (type == "countries") {
          updateQuery = `UPDATE countries SET is_translated = true WHERE id = $1`;
        } else if (type == "tournaments") {
          updateQuery = `UPDATE tournaments SET is_translated = true WHERE id = $1`;
        } else if (type == "competitors") {
          updateQuery = `UPDATE competitors SET is_translated = true WHERE id = $1`;
        } else if (type == "markets") {
          updateQuery = `UPDATE market_constants SET is_translated = true WHERE id = $1`;
        } else if (type == "outcomes") {
          updateQuery = `UPDATE outcome_constants SET is_translated = true WHERE id = $1`;
        }
        await client.query(updateQuery, [row.id]);
      }
      index++;
    }
  } finally {
    client.release();
  }
}

async function makeDictionariesThemes() {
  const client = await pool.connect();
  try {
    const queryText = `SELECT * FROM theme_dictionaries WHERE is_translated=false ORDER BY id`;
    const result = await client.query(queryText);
    const rows = result.rows;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let newBatch = [];
      for (let row of batch) {
        const themeId = row.id;
        const insertQuery = `INSERT INTO dictionaries ("group", group_id, group_ref_id, created_at, updated_at) 
                             VALUES ('admin', $1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *;`;
        const result = await client.query(insertQuery, [themeId]);
        const dictionaryItem = result.rows[0];
        row.dictionaryId = dictionaryItem.id;

        row.key = row.key.replace(/_/g, " ");
        newBatch.push(row);
      }

      await processBatch(newBatch, "themes");
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function makeDictionariesSports() {
  const client = await pool.connect();
  try {
    const queryText = `SELECT * FROM sports WHERE is_translated=false ORDER BY id`;
    const result = await client.query(queryText);
    const rows = result.rows;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let newBatch = [];
      for (let row of batch) {
        const sportId = row.id;
        const sportRefId = row.reference_id;
        const insertQuery = `INSERT INTO dictionaries ("group", group_id, group_ref_id, created_at, updated_at) 
                             VALUES ('sports', $1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *;`;
        const result = await client.query(insertQuery, [sportId, sportRefId]);
        const dictionaryItem = result.rows[0];
        row.dictionaryId = dictionaryItem.id;
        newBatch.push(row);
      }

      await processBatch(newBatch, "sports");
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function makeDictionariesCountries() {
  const client = await pool.connect();
  try {
    const queryText = `SELECT * FROM countries WHERE is_translated=false ORDER BY id`;
    const result = await client.query(queryText);
    const rows = result.rows;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let newBatch = [];
      for (let row of batch) {
        const countryId = row.id;
        const countryRefId = row.reference_id;
        const insertQuery = `INSERT INTO dictionaries ("group", group_id, group_ref_id, created_at, updated_at) 
                             VALUES ('country', $1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *;`;
        const result = await client.query(insertQuery, [
          countryId,
          countryRefId,
        ]);
        const dictionaryItem = result.rows[0];
        row.dictionaryId = dictionaryItem.id;
        newBatch.push(row);
      }

      await processBatch(newBatch, "countries");
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function makeDictionariesTournaments() {
  const client = await pool.connect();
  try {
    const queryText = `SELECT * FROM tournaments WHERE is_translated=false ORDER BY id`;
    const result = await client.query(queryText);
    const rows = result.rows;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let newBatch = [];
      for (let row of batch) {
        const tournamentId = row.id;
        const tournamentRefId = row.reference_id;
        const insertQuery = `INSERT INTO dictionaries ("group", group_id, group_ref_id, created_at, updated_at) 
                             VALUES ('tournament', $1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *;`;
        const result = await client.query(insertQuery, [
          tournamentId,
          tournamentRefId,
        ]);
        const dictionaryItem = result.rows[0];
        row.dictionaryId = dictionaryItem.id;
        newBatch.push(row);
      }

      await processBatch(newBatch, "tournaments");
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function makeDictionariesCompetitors() {
  const client = await pool.connect();
  try {
    const queryText = `SELECT * FROM competitors WHERE is_translated=false ORDER BY id`;
    const result = await client.query(queryText);
    const rows = result.rows;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let newBatch = [];
      for (let row of batch) {
        const competitorId = row.id;
        const competitorRefId = row.reference_id;
        const insertQuery = `INSERT INTO dictionaries ("group", group_id, group_ref_id, created_at, updated_at) 
                             VALUES ('competitor', $1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *;`;
        const result = await client.query(insertQuery, [
          competitorId,
          competitorRefId,
        ]);
        const dictionaryItem = result.rows[0];
        row.dictionaryId = dictionaryItem.id;
        newBatch.push(row);
      }

      await processBatch(newBatch, "competitors");
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function makeDictionariesMarkets() {
  const client = await pool.connect();
  try {
    const queryText = `SELECT * FROM market_constants WHERE is_translated=false ORDER BY id`;
    const result = await client.query(queryText);
    const rows = result.rows;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let newBatch = [];
      for (let row of batch) {
        const marketId = row.id;
        const marketRefId = row.reference_id;
        const insertQuery = `INSERT INTO dictionaries ("group", group_id, group_ref_id, created_at, updated_at) 
                             VALUES ('market', $1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *;`;
        const result = await client.query(insertQuery, [marketId, marketRefId]);
        const dictionaryItem = result.rows[0];
        row.dictionaryId = dictionaryItem.id;
        newBatch.push(row);
      }

      await processBatch(newBatch, "markets");
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function makeDictionariesOutcomes() {
  const client = await pool.connect();
  try {
    const queryText = `SELECT * FROM outcome_constants WHERE is_translated=false ORDER BY id`;
    const result = await client.query(queryText);
    const rows = result.rows;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let newBatch = [];
      for (let row of batch) {
        const outcomeId = row.id;
        const outcomeRefId = row.reference_id;
        const insertQuery = `INSERT INTO dictionaries ("group", group_id, group_ref_id, created_at, updated_at) 
                             VALUES ('outcome', $1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *;`;
        const result = await client.query(insertQuery, [
          outcomeId,
          outcomeRefId,
        ]);
        const dictionaryItem = result.rows[0];
        row.dictionaryId = dictionaryItem.id;
        newBatch.push(row);
      }

      await processBatch(newBatch, "outcomes");
    }
  } catch (err) {
    console.error("An error occurred:", err.message);
  } finally {
    client.release();
  }
}

async function main() {
  // await makeMarketConstants();
  // await makeCountries();
  // await makeSportGroup();
  // await makeDictionaries();

  // await makeSports();
  // await makeCountries();
  // await makeMarketConstants();
  await makeTournaments();
  console.log("Data successfully saved to PostgreSQL database.");
}

main();
