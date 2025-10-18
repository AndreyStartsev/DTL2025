/**
 * /static/js/visualizer.js
 * Contains all logic for parsing DDL and rendering static Mermaid ERD diagrams.
 */

/**
 * Calculate schema statistics from parsed data
 * @param {object} schemaData - Either parsed DDL tables object or schema overview object
 * @returns {object} Statistics { tableCount, pkCount, fkCount }
 */
function calculateSchemaStats(schemaData) {
    let tableCount = 0;
    let pkCount = 0;
    let fkCount = 0;

    // Handle different data structures
    if (schemaData.tables) {
        const tables = Array.isArray(schemaData.tables)
            ? schemaData.tables
            : Object.values(schemaData.tables);

        tableCount = tables.length;

        tables.forEach(table => {
            if (table.columns) {
                const cols = Array.isArray(table.columns)
                    ? table.columns
                    : Object.values(table.columns);

                cols.forEach(col => {
                    if (col.is_primary_key || (col.attributes && col.attributes.includes('PK'))) {
                        pkCount++;
                    }
                    if (col.is_foreign_key || (col.attributes && col.attributes.includes('FK'))) {
                        fkCount++;
                    }
                });
            }
        });
    }

    return { tableCount, pkCount, fkCount };
}

/**
 * Calculate stats from mermaid code
 * @param {string} mermaidCode - The generated Mermaid ERD code
 * @returns {object} Statistics { tableCount, pkCount, fkCount }
 */
function calculateStatsFromMermaid(mermaidCode) {
    const lines = mermaidCode.split('\n');
    let tableCount = 0;
    let pkCount = 0;
    let fkCount = 0;

    lines.forEach(line => {
        // Count tables (lines that start with table name followed by {)
        if (line.trim().match(/^\w+\s*\{$/)) {
            tableCount++;
        }
        // Count PK and FK in column definitions
        if (line.includes('"PK"') || line.includes('PK ')) {
            pkCount++;
        }
        if (line.includes('"FK"') || line.includes('FK ')) {
            fkCount++;
        }
    });

    return { tableCount, pkCount, fkCount };
}

/**
 * Render schema statistics badge
 * @param {object} stats - { tableCount, pkCount, fkCount }
 * @returns {string} HTML string for stats display
 */
function renderSchemaStats(stats) {
    const { tableCount, pkCount, fkCount } = stats;
    return `
        <div class="d-flex gap-2 align-items-center mb-2">
            <span class="badge bg-primary fs-6">
                <i class="bi bi-table me-1"></i>${tableCount} ${tableCount === 1 ? 'table' : 'tables'}
            </span>
            <span class="badge bg-info text-dark">
                <i class="bi bi-key-fill me-1"></i>${pkCount} PK
            </span>
            <span class="badge bg-warning text-dark">
                <i class="bi bi-link-45deg me-1"></i>${fkCount} FK
            </span>
        </div>
    `;
}

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
 * Sanitize column type for Mermaid ERD
 * Removes REFERENCES clauses and problematic characters
 */
function sanitizeColumnType(typeString) {
    if (!typeString) return 'unknown';

    // Remove REFERENCES clause (everything from REFERENCES onwards)
    let cleaned = typeString.replace(/\s+REFERENCES\s+.+$/i, '');

    // Remove other constraints that might have problematic chars
    cleaned = cleaned.replace(/\s+CHECK\s*\(.+?\)/gi, '');
    cleaned = cleaned.replace(/\s+DEFAULT\s+.+?(?=\s+|$)/gi, '');

    // Replace problematic characters for Mermaid
    cleaned = cleaned.replace(/[.\s,()]+/g, '_');

    // Remove trailing/leading underscores
    cleaned = cleaned.replace(/^_+|_+$/g, '');

    // If empty after cleaning, use 'unknown'
    return cleaned || 'unknown';
}

/**
 * Extract column names from SELECT clause of CTAS
 * @param {string} selectClause - The SELECT part of the query
 * @returns {Array<object>} Array of column objects with name and type
 */
function extractColumnsFromSelect(selectClause) {
    const columns = [];

    // Remove leading SELECT keyword
    let cleaned = selectClause.replace(/^\s*SELECT\s+/i, '').trim();

    // Split by comma, but respect function calls and nested selects
    const parts = smartSplitColumns(cleaned);

    parts.forEach(part => {
        part = part.trim();

        // Handle AS aliases: "expression AS alias" or "expression alias"
        let columnName = null;

        // Check for explicit AS
        const asMatch = part.match(/\s+AS\s+([`"]?\w+[`"]?)$/i);
        if (asMatch) {
            columnName = asMatch[1].replace(/[`"]/g, '');
        } else {
            // Check for implicit alias (space-separated)
            // e.g., "table.column alias" or "function() alias"
            const implicitMatch = part.match(/[\w.)]+\s+([`"]?\w+[`"]?)$/);
            if (implicitMatch) {
                columnName = implicitMatch[1].replace(/[`"]/g, '');
            } else {
                // No alias - extract from the expression
                // Handle "table.column" -> "column"
                const dotMatch = part.match(/\.([`"]?\w+[`"]?)$/);
                if (dotMatch) {
                    columnName = dotMatch[1].replace(/[`"]/g, '');
                } else {
                    // Just use the whole thing (simple column name or function)
                    const simpleMatch = part.match(/([`"]?\w+[`"]?)$/);
                    if (simpleMatch) {
                        columnName = simpleMatch[1].replace(/[`"]/g, '');
                    }
                }
            }
        }

        if (columnName && columnName.toUpperCase() !== 'FROM') {
            columns.push({
                name: columnName,
                type: 'derived',
                attributes: []
            });
        }
    });

    return columns;
}

/**
 * --- REWRITTEN DDL PARSER ---
 * This version handles both traditional CREATE TABLE and CREATE TABLE AS SELECT (CTAS)
 */
function parseDdl(ddlString) {
    const tables = {};
    const relationships = [];
    if (!ddlString || typeof ddlString !== 'string') {
        return { tables, relationships };
    }

    // First, try to match CTAS statements
    const ctasRegex = /CREATE\s+TABLE\s+(?:if\s+not\s+exists\s+)?(?:`|")?([\w."]+)(?:`|")?\s+(?:WITH\s*\([^)]+\)\s+)?AS\s+SELECT\s+([\s\S]+?)(?:FROM|;)/gi;
    let ctasMatch;

    while ((ctasMatch = ctasRegex.exec(ddlString)) !== null) {
        const tableName = ctasMatch[1];
        const selectClause = ctasMatch[2];

        if (!tables[tableName]) {
            tables[tableName] = { name: tableName, columns: [] };
        }

        // Extract columns from SELECT
        const columns = extractColumnsFromSelect('SELECT ' + selectClause);
        tables[tableName].columns = columns;

        console.log(`Parsed CTAS table: ${tableName} with ${columns.length} columns`);
    }

    // Then, match traditional CREATE TABLE statements
    const tableStartRegex = /CREATE\s+TABLE\s+(?:if\s+not\s+exists\s+)?(?:`|")?([\w."]+)(?:`|")?\s*\(/gi;
    let match;

    while ((match = tableStartRegex.exec(ddlString)) !== null) {
        const tableName = match[1];

        // Skip if already parsed as CTAS
        if (tables[tableName]) {
            continue;
        }

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

                // Check for PRIMARY KEY
                if (colDetails.toLowerCase().includes('primary key')) {
                    attributes.push('PK');
                    colDetails = colDetails.replace(/primary key/ig, '').trim();
                }

                // Check for FOREIGN KEY (REFERENCES clause)
                if (colDetails.toLowerCase().includes('references')) {
                    attributes.push('FK');
                }

                // Sanitize the column type
                const colType = sanitizeColumnType(colDetails);
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
                const sanitizedType = sanitizeColumnType(col.type);
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
    if (!ddlString) return '%% No DDL provided.';
    const { tables, relationships } = parseDdl(ddlString);
    if (Object.keys(tables).length === 0) return '%% DDL could not be parsed or contains no tables.';

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
        const mermaidFrom = rel.from.replace(/\./g, '_'), mermaidTo = rel.to.replace(/\./g, '_');
        mermaidCode += `    ${mermaidFrom} ||--o{ ${mermaidTo} : "${rel.label}"\n`;
    });
    return mermaidCode;
}


async function renderMermaidDiagram(containerId, mermaidCode, stats = null) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = `<div class="text-center p-4"><div class="spinner-border spinner-border-sm"></div><span class="ms-2">Generating diagram...</span></div>`;

    if (mermaidCode.includes('%%')) {
        container.innerHTML = `<div class="alert alert-warning m-2">${escapeHtml(mermaidCode.replace(/%%/g, '').trim())}</div>`;
        return;
    }

    // Calculate stats if not provided
    if (!stats) {
        stats = calculateStatsFromMermaid(mermaidCode);
    }

    try {
        const { svg } = await mermaid.render(`mermaid-${containerId}-${Date.now()}`, mermaidCode);

        // Render stats badge and SVG
        container.innerHTML = `
            ${renderSchemaStats(stats)}
            <div class="diagram-wrapper">${svg}</div>
        `;

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
        container.innerHTML = `<div class="alert alert-danger m-2">Could not render diagram.</div><pre class="m-2 small">${escapeHtml(mermaidCode)}</pre>`;
    }
}