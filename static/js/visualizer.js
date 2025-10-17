/**
 * /static/js/visualizer.js
 * Contains all logic for parsing DDL and rendering static Mermaid ERD diagrams.
 */

/**
 * Smart column splitting that respects parentheses and quoted strings.
 * Splits by commas but not when inside parentheses or quotes.
 * @param {string} columnsBlock - The columns definition block.
 * @returns {Array<string>} Array of individual column definitions.
 */
function smartSplitColumns(columnsBlock) {
    const columns = [];
    let currentColumn = '';
    let parenDepth = 0;
    let inSingleQuote = false;
    let inDoubleQuote = false;

    columnsBlock = columnsBlock.trim();
    if (columnsBlock.endsWith(',')) {
        columnsBlock = columnsBlock.slice(0, -1);
    }

    for (let i = 0; i < columnsBlock.length; i++) {
        const char = columnsBlock[i];
        const prevChar = i > 0 ? columnsBlock[i - 1] : '';

        if (char === "'" && prevChar !== '\\') {
            if (!inDoubleQuote) inSingleQuote = !inSingleQuote;
        } else if (char === '"' && prevChar !== '\\') {
            if (!inSingleQuote) inDoubleQuote = !inDoubleQuote;
        }

        if (!inSingleQuote && !inDoubleQuote) {
            if (char === '(') {
                parenDepth++;
            } else if (char === ')') {
                parenDepth = Math.max(0, parenDepth - 1);
            }
        }

        if (char === ',' && parenDepth === 0 && !inSingleQuote && !inDoubleQuote) {
            if (currentColumn.trim()) {
                columns.push(currentColumn.trim());
            }
            currentColumn = '';
        } else {
            currentColumn += char;
        }
    }

    if (currentColumn.trim()) {
        columns.push(currentColumn.trim());
    }

    return columns;
}


/**
 * --- REWRITTEN DDL PARSER ---
 * This version uses a robust programmatic approach to find the correct parenthesis
 * scope for a table's column definitions.
 */
function parseDdl(ddlString) {
    const tables = {};
    const relationships = [];
    if (!ddlString || typeof ddlString !== 'string') {
        return { tables, relationships };
    }

    const tableStartRegex = /CREATE TABLE\s+(?:if not exists\s+)?(?:`|")?([\w."]+)(?:`|")?\s*\(/gi;
    let match;

    while ((match = tableStartRegex.exec(ddlString)) !== null) {
        const tableName = match[1];
        const startIndex = match.index + match[0].length;
        let parenDepth = 1;
        let endIndex = -1;

        for (let i = startIndex; i < ddlString.length; i++) {
            if (ddlString[i] === '(') {
                parenDepth++;
            } else if (ddlString[i] === ')') {
                parenDepth--;
            }
            if (parenDepth === 0) {
                endIndex = i;
                break;
            }
        }

        if (endIndex === -1) {
             console.warn(`Could not find matching ')' for table ${tableName}. Skipping.`);
             continue;
        }

        const columnsBlock = ddlString.substring(startIndex, endIndex);
        if (!tables[tableName]) {
            tables[tableName] = { name: tableName, columns: [] };
        }

        const columnDefinitions = smartSplitColumns(columnsBlock);
        columnDefinitions.forEach(line => {
             if (line.toUpperCase().startsWith('PRIMARY KEY') || line.toUpperCase().startsWith('FOREIGN KEY') || line.trim() === '') {
                return;
            }

            const colRegex = /^(?:`|")?(\w+)(?:`|")?\s+(.+)$/i;
            const colMatch = line.match(colRegex);

            if (colMatch) {
                const colName = colMatch[1];
                let colDetails = colMatch[2].trim();
                let attributes = [];

                if (colDetails.toLowerCase().includes('primary key')) {
                    attributes.push('PK');
                    colDetails = colDetails.replace(/primary key/ig, '').trim();
                }
                const colType = colDetails;
                tables[tableName].columns.push({ name: colName, type: colType, attributes });

            } else {
                console.warn(`  [FAIL] Could not parse DDL line: "${line}"`);
            }
        });
        tableStartRegex.lastIndex = endIndex;
    }
    return { tables, relationships };
}

/**
 * --- CORRECTED FOR ORIGINAL SCHEMA ---
 * This function is now more robust and handles different JSON structures for the original schema data.
 * It also logs the received data structure to the console for easier debugging.
 */
function generateMermaidFromSchemaObject(schemaData) {
    console.log("Generating Mermaid for ORIGINAL schema. Received data:", JSON.parse(JSON.stringify(schemaData)));

    if (!schemaData || (!Array.isArray(schemaData.tables) && typeof schemaData.tables !== 'object')) {
        return '%% Original schema data (`schema_overview.tables`) is missing, not an array, or not an object.';
    }

    let mermaidCode = 'erDiagram\n';
    // Handle if schemaData.tables is an array of table objects OR an object of table objects.
    const tables = Array.isArray(schemaData.tables) ? schemaData.tables : Object.values(schemaData.tables);

    tables.forEach(table => {
        if (!table || !table.name) {
            console.warn("Skipping invalid table entry in original schema data:", table);
            return;
        }

        const mermaidTableName = table.name.replace(/\./g, '_');
        mermaidCode += `    ${mermaidTableName} {\n`;

        if (table.columns) {
            // Handle if table.columns is an array OR an object where keys are column names.
            const cols = Array.isArray(table.columns)
                ? table.columns
                : Object.entries(table.columns).map(([name, details]) => ({ ...details, name }));

            cols.forEach(col => {
                if (!col || !col.name || !col.type) {
                     console.warn(`Skipping invalid column in table ${table.name}:`, col);
                    return;
                }
                const pk = col.is_primary_key ? ' PK' : '';
                const fk = col.is_foreign_key ? ' FK' : '';
                const sanitizedType = (col.type || 'unknown').replace(/[\s,]+/g, '_');
                mermaidCode += `        ${sanitizedType} ${col.name} "${(pk + fk).trim()}"\n`;
            });
        }
        mermaidCode += '    }\n';
    });

    if (Array.isArray(schemaData.relations)) {
        schemaData.relations.forEach(rel => {
            if (rel.from_table && rel.to_table) mermaidCode += `    ${rel.from_table.replace(/\./g, '_')} ||--|{ ${rel.to_table.replace(/\./g, '_')} : ""\n`;
        });
    }
    return mermaidCode;
}

function generateMermaidFromDDL(ddlString) {
    if (!ddlString) return '%% No optimized DDL provided.';
    const { tables, relationships } = parseDdl(ddlString);
    if (Object.keys(tables).length === 0) return '%% Optimized DDL could not be parsed or contains no tables.';
    let mermaidCode = 'erDiagram\n';
    for (const tableName in tables) {
        const mermaidTableName = tableName.replace(/\./g, '_');
        mermaidCode += `    ${mermaidTableName} {\n`;
        tables[tableName].columns.forEach(col => {
            const attrs = col.attributes.join(' ').trim();
            const sanitizedType = col.type.replace(/[\s,]+/g, '_');
            mermaidCode += `        ${sanitizedType} ${col.name} ${attrs ? `"${attrs}"` : ''}\n`;
        });
        mermaidCode += '    }\n';
    }
    relationships.forEach(rel => {
        const mermaidFrom = rel.from.replace(/\./g, '_'), mermaidTo = rel.to.replace(/\./g, '_');
        mermaidCode += `    ${mermaidFrom} ||--o{ ${mermaidTo} : "${rel.label}"\n`;
    });
    return mermaidCode;
}


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
        const svgEl = container.querySelector('svg');

        if (svgEl) {
            const initialWidth = svgEl.viewBox.baseVal.width;
            svgEl.style.width = `${initialWidth}px`;
            svgEl.removeAttribute('height');
            svgEl.style.cursor = 'grab';
            let isPanning = false;
            let startPoint = { x: 0, y: 0 };
            svgEl.addEventListener('mousedown', (e) => {
                if (e.button !== 0) return;
                isPanning = true;
                svgEl.style.cursor = 'grabbing';
                startPoint = { x: e.clientX, y: e.clientY };
                e.preventDefault();
            });
            const stopPanning = () => {
                if (!isPanning) return;
                isPanning = false;
                svgEl.style.cursor = 'grab';
            };
            container.addEventListener('mousemove', (e) => {
                if (!isPanning) return;
                e.preventDefault();
                container.scrollLeft -= e.clientX - startPoint.x;
                container.scrollTop -= e.clientY - startPoint.y;
                startPoint = { x: e.clientX, y: e.clientY };
            });
            container.addEventListener('mouseup', stopPanning);
            container.addEventListener('mouseleave', stopPanning);
            container.addEventListener('wheel', (e) => {
                e.preventDefault();
                const scale = e.deltaY < 0 ? 1.15 : 1 / 1.15;
                const currentWidth = svgEl.getBoundingClientRect().width;
                const newWidth = Math.max(100, currentWidth * scale);
                const pointX = e.clientX - container.getBoundingClientRect().left;
                const pointY = e.clientY - container.getBoundingClientRect().top;
                const scrollXRatio = (container.scrollLeft + pointX) / currentWidth;
                const scrollYRatio = (container.scrollTop + pointY) / svgEl.getBoundingClientRect().height;
                svgEl.style.width = `${newWidth}px`;
                const newHeight = svgEl.getBoundingClientRect().height;
                container.scrollLeft = (scrollXRatio * newWidth) - pointX;
                container.scrollTop = (scrollYRatio * newHeight) - pointY;
            });
        }
    } catch (error) {
        console.error(`Mermaid rendering failed for #${containerId}:`, error);
        container.innerHTML = `<div class="alert alert-danger m-2">Could not render diagram.</div><pre class="m-2">${escapeHtml(mermaidCode)}</pre>`;
    }
}