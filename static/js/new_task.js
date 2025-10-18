/**
 * /static/js/create_task.js
 * Handles new task creation form and submission
 */

// Strategy descriptions
const strategyDescriptions = {
    'read_optimized': '<strong>Read Optimized:</strong> Maximizes query performance with denormalization and indexing strategies for analytics-heavy workloads',
    'write_optimized': '<strong>Write Optimized:</strong> Minimizes write overhead with normalized schemas, ideal for transactional systems with frequent updates',
    'balanced': '<strong>Balanced:</strong> Optimizes for both read and write operations, suitable for mixed workloads',
    'storage_optimized': '<strong>Storage Optimized:</strong> Reduces disk space usage through compression and efficient data types, best for cost-sensitive applications'
};

/**
 * Initialize strategy description updater
 */
function initializeStrategySelector() {
    const strategySelect = document.getElementById('strategySelect');
    const strategyDescription = document.getElementById('strategyDescription');

    if (strategySelect && strategyDescription) {
        strategySelect.addEventListener('change', function() {
            const selectedStrategy = this.value;
            strategyDescription.innerHTML = strategyDescriptions[selectedStrategy] || '';
        });
    }
}

/**
 * Validate task data JSON
 * @param {object} data - Parsed JSON data
 * @throws {Error} if validation fails
 */
function validateTaskData(data) {
    if (!data.url || typeof data.url !== 'string') {
        throw new Error('Missing or invalid "url" field. Must be a valid database connection string.');
    }

    if (!data.ddl || !Array.isArray(data.ddl)) {
        throw new Error('Missing or invalid "ddl" field. Must be an array of DDL statements.');
    }

    if (data.ddl.length === 0) {
        throw new Error('The "ddl" array cannot be empty. Provide at least one CREATE TABLE statement.');
    }

    if (!data.queries || !Array.isArray(data.queries)) {
        throw new Error('Missing or invalid "queries" field. Must be an array of query objects.');
    }

    if (data.queries.length === 0) {
        throw new Error('The "queries" array cannot be empty. Provide at least one query to optimize.');
    }

    // Validate DDL structure
    data.ddl.forEach((ddl, index) => {
        if (!ddl.statement || typeof ddl.statement !== 'string') {
            throw new Error(`DDL item at index ${index} is missing "statement" field or it's not a string.`);
        }
    });

    // Validate queries structure - check for 'queryid' and 'query' fields
    data.queries.forEach((queryObj, index) => {
        if (!queryObj.queryid || typeof queryObj.queryid !== 'string') {
            throw new Error(`Query at index ${index} is missing "queryid" field or it's not a string.`);
        }

        if (!queryObj.query || typeof queryObj.query !== 'string') {
            throw new Error(`Query at index ${index} is missing "query" field or it's not a string.`);
        }
    });
}

/**
 * Submit new task creation
 * @param {Event} e - Form submit event
 * @param {Function} onSuccess - Callback on successful submission
 */
async function handleCreateTaskSubmit(e, onSuccess) {
    e.preventDefault();

    const createSubmitBtn = document.getElementById('createSubmitBtn');
    const createSpinner = document.getElementById('createSpinner');
    const createError = document.getElementById('createError');
    const createErrorMessage = document.getElementById('createErrorMessage');
    const taskDataJson = document.getElementById('taskDataJson');
    const strategySelect = document.getElementById('strategySelect');
    const modelSelect = document.getElementById('modelSelect');

    // Disable submit button and show spinner
    createSubmitBtn.disabled = true;
    createSpinner.classList.remove('d-none');
    createError.classList.add('d-none');

    try {
        // Parse and validate JSON
        let data;
        try {
            data = JSON.parse(taskDataJson.value);
        } catch (parseError) {
            throw new Error(`Invalid JSON format: ${parseError.message}`);
        }

        // Validate task data
        validateTaskData(data);

        // Build payload
        const payload = {
            ...data,
            config: {
                strategy: strategySelect.value,
                model_id: modelSelect.value,
                context_length: 16000,
                batch_size: 5
            }
        };

        // Submit to API
        const response = await fetch('/new', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (response.ok) {
            // Success - hide modal and reset form
            const modal = bootstrap.Modal.getInstance(document.getElementById('createTaskModal'));
            if (modal) modal.hide();

            // Reset form
            document.getElementById('createTaskForm').reset();

            // Reset strategy description to default
            const strategyDescription = document.getElementById('strategyDescription');
            if (strategyDescription) {
                strategyDescription.innerHTML = strategyDescriptions['balanced'];
            }

            // Call success callback
            if (typeof onSuccess === 'function') {
                onSuccess();
            }
        } else {
            // Handle API error
            let errorData;
            try {
                errorData = await response.json();
            } catch {
                errorData = { detail: `Server error: ${response.status} ${response.statusText}` };
            }
            throw new Error(errorData.detail || 'Failed to create task.');
        }
    } catch (error) {
        // Display error
        createErrorMessage.textContent = error.message;
        createError.classList.remove('d-none');
        console.error('Error creating task:', error);
    } finally {
        // Re-enable submit button and hide spinner
        createSubmitBtn.disabled = false;
        createSpinner.classList.add('d-none');
    }
}

/**
 * Initialize create task form
 * @param {Function} onSuccess - Callback on successful task creation
 */
function initializeCreateTaskForm(onSuccess) {
    const createTaskForm = document.getElementById('createTaskForm');

    if (createTaskForm) {
        createTaskForm.addEventListener('submit', (e) => handleCreateTaskSubmit(e, onSuccess));
    }

    // Initialize strategy selector
    initializeStrategySelector();
}