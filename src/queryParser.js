function parseQuery3(query) {
    const selectRegex = /SELECT (.+) FROM (.+)/i;
    const match = query.match(selectRegex);

    if (match) {
        const [, fields, table] = match;
        return {
            fields: fields.split(',').map(field => field.trim()),
            table: table.trim()
        };
    } else {
        throw new Error('Invalid query format');
    }
}
module.exports = parseQuery3;

function parseQuery5(query) {
    const selectRegex = /SELECT (.+?) FROM (.+?)(?: WHERE (.*))?$/i;
    const match = query.match(selectRegex);

    if (match) {
        const [, fields, table, whereClause] = match;
        return {
            fields: fields.split(',').map(field => field.trim()),
            table: table.trim(),
            whereClause: whereClause ? whereClause.trim() : null
        };
    } else {
        throw new Error('Invalid query format');
    }
}
module.exports = parseQuery5;

function parseQuery6(query) {
    const selectRegex = /SELECT (.+?) FROM (.+?)(?: WHERE (.*))?$/i;
    const match = query.match(selectRegex);

    if (match) {
        const [, fields, table, whereString] = match;
        const whereClauses = whereString ? parseWhereClause6(whereString) : [];
        return {
            fields: fields.split(',').map(field => field.trim()),
            table: table.trim(),
            whereClauses
        };
    } else {
        throw new Error('Invalid query format');
    }
}

function parseWhereClause6(whereString) {
    const conditions = whereString.split(/ AND | OR /i);
    return conditions.map(condition => {
        const [field, operator, value] = condition.split(/\s+/);
        return { field, operator, value };
    });
}
module.exports = parseQuery6;

function parseWhereClause7(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}
