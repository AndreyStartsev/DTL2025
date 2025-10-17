/**
 * /static/js/visualizer.js
 * Contains all logic for parsing DDL and rendering Mermaid ERD diagrams.
 */

/**
 * Parses a DDL string and extracts table and relationship information.
 * @param {string} ddlString - The DDL SQL string to parse.
 * @returns {object} An object with `tables` and `relationships` arrays.
 */
function parseDdl(ddlString) {
    const tables = {};
    const relationships = [];
    if (!ddlString || typeof ddlString !== 'string') return { tables, relationships };

    const statements = ddlString.split(';').filter(s => s.trim().toUpperCase().startsWith('CREATE TABLE'));

    statements.forEach(statement => {
        const tableRegex = /CREATE TABLE\s+(?:if not exists\s+)?(?:`|")?([\w."]+)(?:`|")?\s*\(([\s\S]*)/i;
        const tableMatch = statement.match(tableRegex);
        if (!tableMatch) return;

        const tableName = tableMatch[1];
        const restOfString = tableMatch[2];

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
        if (endIndex === -1) return;

        const columnsBlock = restOfString.substring(0, endIndex);
        tables[tableName] = { name: tableName, columns: [] };

        columnsBlock.split('\n').forEach(line => {
            const trimmedLine = line.trim().replace(/,$/, '');
            if (!trimmedLine || trimmedLine.startsWith('--') || trimmedLine.startsWith(')')) return;

            const colMatch = trimmedLine.match(/^(?:`|")?(\w+)(?:`|")?\s+([\w\s().]+)/);
            if (colMatch) {
                const colName = colMatch[1];
                let colType = colMatch[2].trim().split(' ')[0];
                if (colType.includes('(')) colType = colType.substring(0, colType.indexOf('('));

                let attributes = [];
                if (trimmedLine.toLowerCase().includes('primary key')) attributes.push('PK');

                tables[tableName].columns.push({
                    name: colName,
                    type: colType,
                    attributes
                });
            }

            const fkRegex = /FOREIGN KEY\s*\((?:`|")?(\w+)(?:`|")?\)\s*REFERENCES\s*(?:`|")?([\w."]+)(?:`|")?/i;
            const fkMatch = trimmedLine.match(fkRegex);
            if (fkMatch) {
                const fromColumn = fkMatch[1];
                const toTable = fkMatch[2];
                relationships.push({
                    from: tableName,
                    to: toTable,
                    label: `${fromColumn} ->`
                });

                const col = tables[tableName].columns.find(c => c.name === fromColumn);
                if (col && !col.attributes.includes('FK')) col.attributes.push('FK');
            }
        });
    });

    return { tables, relationships };
}

/**
 * Generates Mermaid ERD code from a schema object (from analysis report).
 * @param {object} schemaData - The schema overview object with tables and relations.
 * @returns {string} Mermaid diagram code.
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
                mermaidCode += `        ${col.type || 'unknown'} ${col.name || 'unnamed'} "${pk}${fk}"\n`;
            });
        }

        mermaidCode += '    }\n';
    });

    if (Array.isArray(schemaData.relations)) {
        schemaData.relations.forEach(rel => {
            if (rel.from_table && rel.to_table) {
                mermaidCode += `    ${rel.from_table.replace(/\./g, '_')} ||--|{ ${rel.to_table.replace(/\./g, '_')} : ""\n`;
            }
        });
    }

    return mermaidCode;
}

/**
 * Generates Mermaid ERD code from a DDL string.
 * @param {string} ddlString - The DDL SQL string.
 * @returns {string} Mermaid diagram code.
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
            mermaidCode += `        ${col.type} ${col.name} ${attrs ? `"${attrs}"` : ''}\n`;
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
 * Renders a Mermaid diagram in a specified container.
 * @param {string} containerId - The ID of the container element.
 * @param {string} mermaidCode - The Mermaid diagram code.
 */
async function renderMermaidDiagram(containerId, mermaidCode) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = `<div class="text-center p-4"><div class="spinner-border spinner-border-sm"></div><span class="ms-2">Generating diagram...</span></div>`;

    if (mermaidCode.includes('%%')) {
        container.innerHTML = `<div class="alert alert-warning m-2">${escapeHtml(mermaidCode.replace(/%%/g, '').trim())}</div>`;
        return;
    }

    try {
        const { svg } = await mermaid.render(`mermaid-${containerId}-${Date.now()}`, mermaidCode);
        container.innerHTML = svg;

        // Add zoom on click for the SVG
        const svgElement = container.querySelector('svg');
        if (svgElement) {
            svgElement.style.cursor = 'pointer';
            svgElement.style.maxWidth = '100%';
            svgElement.style.height = 'auto';
        }
    } catch (error) {
        console.error(`Mermaid rendering failed for #${containerId}:`, error);
        container.innerHTML = `<div class="alert alert-danger m-2">Could not render diagram.</div><pre class="m-2">${escapeHtml(mermaidCode)}</pre>`;
    }
}