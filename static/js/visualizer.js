// /static/js/visualizer.js

/**
 * Parses a raw DDL string into a structured object using a robust multi-step method.
 * @param {string} ddlString - The raw DDL string potentially containing multiple statements.
 * @returns {{tables: object, relationships: Array}}
 */
function parseDdl(ddlString) {
    const tables = {};
    const relationships = [];

    // 1. Split the entire DDL string into individual statements.
    const statements = ddlString.split(';').filter(s => s.trim().toUpperCase().startsWith('CREATE TABLE'));

    // 2. Process each statement individually.
    statements.forEach(statement => {
        // Use a non-global regex to find the table name and the start of the column block.
        const tableRegex = /CREATE TABLE\s+(?:if not exists\s+)?(?:`|")?([\w."]+)(?:`|")?\s*\(([\s\S]*)/i;
        const tableMatch = statement.match(tableRegex);

        if (!tableMatch) return; // Continue to the next statement if this one is malformed.

        const tableName = tableMatch[1];
        const restOfString = tableMatch[2];

        // 3. Find the balanced closing parenthesis for the column definition block.
        let openParens = 1;
        let endIndex = -1;
        for (let i = 0; i < restOfString.length; i++) {
            if (restOfString[i] === '(') openParens++;
            else if (restOfString[i] === ')') openParens--;

            if (openParens === 0) {
                endIndex = i;
                break;
            }
        }

        if (endIndex === -1) return; // Malformed DDL, skip to the next statement.

        const columnsBlock = restOfString.substring(0, endIndex);

        tables[tableName] = { name: tableName, columns: [] };

        // 4. Parse the isolated column block.
        const lines = columnsBlock.split('\n');
        lines.forEach(line => {
            const trimmedLine = line.trim().replace(/,$/, '');
            if (!trimmedLine || trimmedLine.startsWith('--') || trimmedLine.startsWith(')')) return;

            const colMatch = trimmedLine.match(/^(?:`|")?(\w+)(?:`|")?\s+([\w\s().]+)/);
            if (colMatch) {
                const colName = colMatch[1];
                let colType = colMatch[2].trim().split(' ')[0]; // Get the first part of the type.

                if (colType.includes('(')) {
                   colType = colType.substring(0, colType.indexOf('('));
                }

                let attributes = [];
                if (trimmedLine.toLowerCase().includes('primary key')) attributes.push('PK');

                tables[tableName].columns.push({ name: colName, type: colType, attributes });
            }

            const fkRegex = /FOREIGN KEY\s*\((?:`|")?(\w+)(?:`|")?\)\s*REFERENCES\s*(?:`|")?([\w."]+)(?:`|")?/i;
            const fkMatch = trimmedLine.match(fkRegex);
            if (fkMatch) {
                const fromColumn = fkMatch[1];
                const toTable = fkMatch[2];
                relationships.push({ from: tableName, to: toTable, label: `${fromColumn} ->` });

                const col = tables[tableName].columns.find(c => c.name === fromColumn);
                if (col && !col.attributes.includes('FK')) {
                    col.attributes.push('FK');
                }
            }
        });
    });

    return { tables, relationships };
}


/**
 * Generates Mermaid ERD syntax from a pre-parsed schema object.
 * @param {object} schemaData - The schema object (e.g., from analysis_report.schema_overview).
 * @returns {string} Mermaid code.
 */
function generateMermaidFromSchemaObject(schemaData) {
    if (!schemaData || !Array.isArray(schemaData.tables) || schemaData.tables.length === 0) {
        return '%% Original schema data not available or is empty.';
    }

    let mermaidCode = 'erDiagram\n';

    schemaData.tables.forEach(table => {
        const mermaidTableName = table.name.replace(/\./g, '_');
        mermaidCode += `    ${mermaidTableName} {\n`;

        if (Array.isArray(table.columns)) {
            table.columns.forEach(col => {
                const pk = col.is_primary_key ? ' PK' : '';
                const fk = col.is_foreign_key ? ' FK' : '';
                const colType = col.type || 'unknown';
                const colName = col.name || 'unnamed';
                mermaidCode += `        ${colType} ${colName} "${pk}${fk}"\n`;
            });
        }
        mermaidCode += '    }\n';
    });

    if (Array.isArray(schemaData.relations)) {
        schemaData.relations.forEach(rel => {
            if (rel.from_table && rel.to_table) {
                const mermaidFromTable = rel.from_table.replace(/\./g, '_');
                const mermaidToTable = rel.to_table.replace(/\./g, '_');
                mermaidCode += `    ${mermaidFromTable} ||--|{ ${mermaidToTable} : ""\n`;
            }
        });
    }

    return mermaidCode;
}

/**
 * Generates Mermaid ERD syntax from a raw DDL string.
 * @param {string} ddlString - The raw DDL string.
 * @returns {string} Mermaid code.
 */
function generateMermaidFromDDL(ddlString) {
    if (!ddlString) {
        return '%% No optimized DDL provided.';
    }

    const { tables, relationships } = parseDdl(ddlString);
    if (Object.keys(tables).length === 0) {
        return '%% Optimized DDL could not be parsed or contains no tables.';
    }

    let mermaidCode = 'erDiagram\n';

    for (const tableName in tables) {
        const mermaidTableName = tableName.replace(/\./g, '_');
        mermaidCode += `    ${mermaidTableName} {\n`;
        tables[tableName].columns.forEach(col => {
            const attrs = col.attributes.join(' ').trim();
            mermaidCode += `        ${col.type.replace(/\(.*\)/, '')} ${col.name} ${attrs ? `"${attrs}"` : ''}\n`;
        });
        mermaidCode += '    }\n';
    }

    relationships.forEach(rel => {
        const mermaidFrom = rel.from.replace(/\./g, '_');
        const mermaidTo = rel.to.replace(/\./g, '_');
        mermaidCode += `    ${mermaidFrom} ||--o{ ${mermaidTo} : "${rel.label}"\n`;
    });

    return mermaidCode;
}

/**
 * Renders a Mermaid diagram into a specified container.
 * @param {string} containerId - The ID of the DOM element to render into.
 * @param {string} mermaidCode - The Mermaid syntax code.
 */
async function renderMermaidDiagram(containerId, mermaidCode) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = `<div class="text-center p-4"><div class="spinner-border spinner-border-sm" role="status"></div><span class="ms-2">Generating diagram...</span></div>`;

    if (mermaidCode.includes('%%')) {
        const message = mermaidCode.replace(/%%/g, '').trim();
        container.innerHTML = `<div class="alert alert-warning m-2">${escapeHtml(message)}</div>`;
        return;
    }

    try {
        const { svg } = await mermaid.render(`mermaid-${containerId}-${Date.now()}`, mermaidCode);
        container.innerHTML = svg;
    } catch (error) {
        console.error(`Mermaid rendering failed for #${containerId}:`, error);
        container.innerHTML = `<div class="alert alert-danger m-2">Could not render diagram. Please check the console for details.</div><pre class="m-2">${escapeHtml(mermaidCode)}</pre>`;
    }
}