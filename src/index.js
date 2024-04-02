const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery4(query) {
    const { fields, table } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);
    
    // Filter the fields based on the query
    return data.map(row => {
        const filteredRow = {};
        fields.forEach(field => {
            filteredRow[field] = row[field];
        });
        return filteredRow;
    });
}
module.exports = executeSELECTQuery4;

async function executeSELECTQuery5(query) {
    const { fields, table, whereClause } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);
    
    // Filtering based on WHERE clause
    const filteredData = whereClause
        ? data.filter(row => {
            const [field, value] = whereClause.split('=').map(s => s.trim());
            return row[field] === value;
        })
        : data;

    // Selecting the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}
module.exports = executeSELECTQuery5;

async function executeSELECTQuery6(query) {
    const { fields, table, whereClauses } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);

    //Apply WHERE clause filtering
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => {
            // You can expand this to handle different operators
            return row[clause.field] === clause.value;
        }))
        : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}
module.exports = executeSELECTQuery6;

async function executeSELECTQuery7(query) {
    const { fields, table, whereClauses } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);

    function evaluateCondition(row, clause) {
        const { field, operator, value } = clause;
        switch (operator) {
            case '=': return row[field] === value;
            case '!=': return row[field] !== value;
            case '>': return row[field] > value;
            case '<': return row[field] < value;
            case '>=': return row[field] >= value;
            case '<=': return row[field] <= value;
            default: throw new Error(`Unsupported operator: ${operator}`);
        }
    }

    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}
module.exports = executeSELECTQuery7;
