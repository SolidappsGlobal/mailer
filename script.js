// Global variables
let agentsData = [];
let filteredData = [];
let selectedAgents = new Set();
let sortColumn = null;
let sortDirection = 'asc';
let activeFilters = {};
let filterOptions = {};
let agentAdvancedFilter = {
    condition: 'contains',
    value: '',
    active: false
};
let pinnedColumns = new Set();
let columnVisibility = {};

// Configuration
const CONFIG = {
    BUBBLE_API_URL: 'https://myunitrust.com/version-live/api/1.1/obj/prelicensingcsv',
    BUBBLE_TOKEN: 'eafe2749ca27a1c37ccf000431c2d083',
    BACK4APP_API_URL: 'https://enqueuecsv-nembb96l.b4a.run',
    BACK4APP_TOKEN: 'eafe2749ca27a1c37ccf000431c2d083'
};

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

function initializeApp() {
    setupEventListeners();
    loadAgentsData();
}

function setupEventListeners() {
    // Export CSV button
    document.getElementById('export-csv').addEventListener('click', exportToCSV);
    
    // Clear filters button
    document.getElementById('clear-filters').addEventListener('click', clearFilters);
    
    // Table sorting
    document.querySelectorAll('.sortable').forEach(header => {
        header.addEventListener('click', () => sortTable(header.dataset.column));
    });
}

// Load agents data from API
async function loadAgentsData() {
    showLoading(true);
    
    try {
        // Try to load from Bubble API first
        const bubbleData = await loadFromBubbleAPI();
        if (bubbleData && bubbleData.length > 0) {
            agentsData = processBubbleData(bubbleData);
        } else {
            // Fallback to sample data
            agentsData = getSampleData();
        }
        
        filteredData = [...agentsData];
        renderTable();
        updateCounts();
        
    } catch (error) {
        console.error('Error loading data:', error);
        // Use sample data as fallback
        agentsData = getSampleData();
        filteredData = [...agentsData];
        renderTable();
        updateCounts();
    } finally {
        showLoading(false);
    }
}

// Load data from Bubble API
async function loadFromBubbleAPI() {
    try {
        const response = await fetch(CONFIG.BUBBLE_API_URL, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${CONFIG.BUBBLE_TOKEN}`,
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        return data.response?.results || [];
        
    } catch (error) {
        console.error('Bubble API error:', error);
        return null;
    }
}

// Process Bubble API data
function processBubbleData(bubbleData) {
    return bubbleData.map(agent => ({
        id: agent._id || agent.id,
        name: agent.first_name && agent.last_name ? 
            `${agent.first_name} ${agent.last_name}` : 
            agent.pre_licensing_email || 'Unknown',
        role: agent.imo || 'Agent',
        ufg: agent.imo || 'N/A',
        preLicenseEnrollment: agent.date_enrolled ? 'Yes' : 'No',
        licensed: 'No', // Default value
        preLicensePercent: agent.percentage_ple_complete ? 
            `${agent.percentage_ple_complete}%` : '-',
        enrollmentDate: agent.date_enrolled ? 
            formatDate(agent.date_enrolled) : '-',
        finishDate: agent.ple_date_completed ? 
            formatDate(agent.ple_date_completed) : '-',
        timeSpent: agent.time_spent_in_course || '-',
        lastLogin: agent.pre_licensing_course_last_login ? 
            formatDate(agent.pre_licensing_course_last_login) : '-',
        courseName: agent.pre_licensing_course || '-',
        preparedToPass: agent.prepared_to_pass || 'No',
        phone: agent.phone || '-',
        email: agent.pre_licensing_email || '-',
        hiringManager: agent.hiring_manager
    }));
}

// Sample data for demonstration
function getSampleData() {
    return [
        {
            id: 1,
            name: 'Aanya Bennett',
            role: 'ADMIN',
            ufg: 'UFG211932',
            preLicenseEnrollment: 'Yes',
            licensed: 'No',
            preLicensePercent: '75%',
            enrollmentDate: '2024-01-01',
            finishDate: '-',
            timeSpent: '45h 30m',
            lastLogin: '2024-01-15',
            courseName: 'Pre-Licensing Course A',
            preparedToPass: 'Yes',
            phone: '(555) 123-4567',
            email: 'aanya.bennett@example.com',
            hiringManager: 'John Smith'
        },
        {
            id: 2,
            name: 'Adam McNicholas',
            role: 'ADMIN',
            ufg: 'UFG226766',
            preLicenseEnrollment: 'Yes',
            licensed: 'No',
            preLicensePercent: '50%',
            enrollmentDate: '2024-01-02',
            finishDate: '-',
            timeSpent: '32h 15m',
            lastLogin: '2024-01-14',
            courseName: 'Pre-Licensing Course B',
            preparedToPass: 'No',
            phone: '(555) 234-5678',
            email: 'adam.mcnicholas@example.com',
            hiringManager: 'Jane Doe'
        },
        {
            id: 3,
            name: 'Brianne Brege',
            role: 'ADMIN',
            ufg: 'UFG226675',
            preLicenseEnrollment: 'Yes',
            licensed: 'No',
            preLicensePercent: '90%',
            enrollmentDate: '2024-01-03',
            finishDate: '-',
            timeSpent: '67h 45m',
            lastLogin: '2024-01-13',
            courseName: 'Pre-Licensing Course A',
            preparedToPass: 'Yes',
            phone: '(555) 345-6789',
            email: 'brianne.brege@example.com',
            hiringManager: 'Mike Johnson'
        },
        {
            id: 4,
            name: 'Jonathan Florez',
            role: 'Agent',
            ufg: 'UFG234981',
            preLicenseEnrollment: 'Yes',
            licensed: 'No',
            preLicensePercent: '25%',
            enrollmentDate: '2024-01-04',
            finishDate: '-',
            timeSpent: '18h 20m',
            lastLogin: '2024-01-12',
            courseName: 'Pre-Licensing Course C',
            preparedToPass: 'No',
            phone: '(555) 456-7890',
            email: 'jonathan.florez@example.com',
            hiringManager: 'Sarah Wilson'
        },
        {
            id: 5,
            name: 'Kayla Curtis',
            role: 'ADMIN',
            ufg: 'UFG219234',
            preLicenseEnrollment: 'Yes',
            licensed: 'No',
            preLicensePercent: '100%',
            enrollmentDate: '2024-01-05',
            finishDate: '2024-01-20',
            timeSpent: '85h 10m',
            lastLogin: '2024-01-11',
            courseName: 'Pre-Licensing Course B',
            preparedToPass: 'Yes',
            phone: '(555) 567-8901',
            email: 'kayla.curtis@example.com',
            hiringManager: 'David Brown'
        },
        {
            id: 6,
            name: 'Pete Beckman',
            role: 'BaseShop',
            ufg: 'UFG205464',
            preLicenseEnrollment: 'Yes',
            licensed: 'No',
            preLicensePercent: '60%',
            enrollmentDate: '2024-01-06',
            finishDate: '-',
            timeSpent: '42h 35m',
            lastLogin: '2024-01-10',
            courseName: 'Pre-Licensing Course A',
            preparedToPass: 'No',
            phone: '(555) 678-9012',
            email: 'pete.beckman@example.com',
            hiringManager: 'Lisa Davis'
        },
        {
            id: 7,
            name: 'Tiberio Maia',
            role: 'Agent',
            ufg: 'ADMIN',
            preLicenseEnrollment: 'Yes',
            licensed: 'No',
            preLicensePercent: '80%',
            enrollmentDate: '2024-01-07',
            finishDate: '-',
            timeSpent: '58h 25m',
            lastLogin: '2024-01-09',
            courseName: 'Pre-Licensing Course C',
            preparedToPass: 'Yes',
            phone: '(555) 789-0123',
            email: 'tiberio.maia@example.com',
            hiringManager: 'Robert Taylor'
        }
    ];
}

// Render the table
function renderTable() {
    const tbody = document.getElementById('agents-tbody');
    const noData = document.getElementById('no-data');
    
    if (filteredData.length === 0) {
        tbody.innerHTML = '';
        noData.style.display = 'flex';
        return;
    }
    
    noData.style.display = 'none';
    
    tbody.innerHTML = filteredData.map(agent => `
        <tr data-agent-id="${agent.id}" class="${selectedAgents.has(agent.id) ? 'selected' : ''}">
            <td>
                <div class="agent-cell">
                    <input type="checkbox" class="agent-checkbox" ${selectedAgents.has(agent.id) ? 'checked' : ''} 
                           onchange="toggleSelection(${agent.id})">
                    <div class="agent-avatar">
                        ${getInitials(agent.name)}
                    </div>
                    <div class="agent-info">
                        <div class="agent-name">${agent.name}</div>
                        <div class="agent-role">- ${agent.role}</div>
                    </div>
                </div>
            </td>
            <td>
                <span class="status-dash">${agent.ufg}</span>
            </td>
            <td>
                <span class="status-badge status-${agent.preLicenseEnrollment.toLowerCase()}">
                    ${agent.preLicenseEnrollment}
                </span>
            </td>
            <td>
                <span class="status-badge status-${agent.licensed.toLowerCase()}">
                    ${agent.licensed}
                </span>
            </td>
            <td>
                <span class="status-dash">${agent.preLicensePercent}</span>
            </td>
            <td>
                <span class="status-dash">${agent.enrollmentDate}</span>
            </td>
            <td>
                <span class="status-dash">${agent.finishDate}</span>
            </td>
            <td>
                <span class="status-dash">${agent.timeSpent}</span>
            </td>
            <td>
                <span class="status-dash">${agent.lastLogin}</span>
            </td>
            <td>
                <span class="status-dash">${agent.courseName}</span>
            </td>
            <td>
                <span class="status-badge status-${agent.preparedToPass.toLowerCase()}">
                    ${agent.preparedToPass}
                </span>
            </td>
            <td>
                <span class="status-dash">${agent.phone}</span>
            </td>
        </tr>
    `).join('');
}

// Get initials from name
function getInitials(name) {
    return name.split(' ').map(n => n[0]).join('').toUpperCase().substring(0, 2);
}

// Toggle agent selection
function toggleSelection(agentId) {
    if (selectedAgents.has(agentId)) {
        selectedAgents.delete(agentId);
    } else {
        selectedAgents.add(agentId);
    }
    
    // Update row appearance
    const row = document.querySelector(`tr[data-agent-id="${agentId}"]`);
    if (row) {
        row.classList.toggle('selected', selectedAgents.has(agentId));
    }
    
    updateCounts();
}

// Update counts
function updateCounts() {
    document.getElementById('total-count').textContent = agentsData.length;
    document.getElementById('selected-count').textContent = selectedAgents.size;
}

// Apply filters
function applyFilters() {
    const searchTerm = document.getElementById('search-agent').value.toLowerCase();
    const licensedFilter = document.getElementById('filter-licensed').value;
    const enrollmentFilter = document.getElementById('filter-enrollment').value;
    
    filteredData = agentsData.filter(agent => {
        const matchesSearch = agent.name.toLowerCase().includes(searchTerm) ||
                            agent.ufg.toLowerCase().includes(searchTerm) ||
                            agent.email.toLowerCase().includes(searchTerm);
        
        const matchesLicensed = !licensedFilter || agent.licensed === licensedFilter;
        const matchesEnrollment = !enrollmentFilter || agent.preLicenseEnrollment === enrollmentFilter;
        
        return matchesSearch && matchesLicensed && matchesEnrollment;
    });
    
    renderTable();
    updateCounts();
}

// Clear all filters
function clearFilters() {
    // Clear advanced Agent filter
    agentAdvancedFilter.condition = 'contains';
    agentAdvancedFilter.value = '';
    agentAdvancedFilter.active = false;
    
    // Reset Agent filter UI
    if (document.getElementById('agent-condition')) {
        document.getElementById('agent-condition').value = 'contains';
    }
    if (document.getElementById('search-agent-filter')) {
        document.getElementById('search-agent-filter').value = '';
    }
    
    // Clear advanced filters
    Object.keys(activeFilters).forEach(column => {
        activeFilters[column].clear();
    });
    
    // Uncheck all filter checkboxes
    document.querySelectorAll('.filter-option input[type="checkbox"]').forEach(checkbox => {
        checkbox.checked = false;
    });
    
    // Close all filter dropdowns
    closeAllFilters();
    
    filteredData = [...agentsData];
    renderTable();
    updateCounts();
    
    // Update filter icon states
    updateFilterIconStates();
}

// Sort table
function sortTable(column) {
    if (sortColumn === column) {
        sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        sortColumn = column;
        sortDirection = 'asc';
    }
    
    filteredData.sort((a, b) => {
        let aVal = a[column];
        let bVal = b[column];
        
        // Handle special cases
        if (column === 'agent') {
            aVal = a.name;
            bVal = b.name;
        } else if (column === 'preLicensePercent') {
            aVal = parseFloat(a.preLicensePercent) || 0;
            bVal = parseFloat(b.preLicensePercent) || 0;
        } else if (column === 'lastLogin') {
            aVal = new Date(a.lastLogin) || new Date(0);
            bVal = new Date(b.lastLogin) || new Date(0);
        }
        
        if (typeof aVal === 'string') {
            aVal = aVal.toLowerCase();
            bVal = bVal.toLowerCase();
        }
        
        if (sortDirection === 'asc') {
            return aVal > bVal ? 1 : -1;
        } else {
            return aVal < bVal ? 1 : -1;
        }
    });
    
    renderTable();
}

// Export to CSV
function exportToCSV() {
    const dataToExport = selectedAgents.size > 0 ? 
        filteredData.filter(agent => selectedAgents.has(agent.id)) : 
        filteredData;
    
    if (dataToExport.length === 0) {
        alert('No data to export');
        return;
    }
    
    const headers = [
        'Agent', 'UFG', 'Pre-License Enrollment', 'Licensed', 'Pre-License %',
        'Enrollment Date', 'Finish Date', 'Time Spent', 'Last Login', 
        'Course Name', 'Prepared To Pass', 'Phone', 'Email', 'Hiring Manager'
    ];
    
    const csvContent = [
        headers.join(','),
        ...dataToExport.map(agent => [
            `"${agent.name}"`,
            `"${agent.ufg}"`,
            `"${agent.preLicenseEnrollment}"`,
            `"${agent.licensed}"`,
            `"${agent.preLicensePercent}"`,
            `"${agent.enrollmentDate}"`,
            `"${agent.finishDate}"`,
            `"${agent.timeSpent}"`,
            `"${agent.lastLogin}"`,
            `"${agent.courseName}"`,
            `"${agent.preparedToPass}"`,
            `"${agent.phone}"`,
            `"${agent.email}"`,
            `"${agent.hiringManager}"`
        ].join(','))
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    link.setAttribute('href', url);
    link.setAttribute('download', `agents_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// Show/hide loading
function showLoading(show) {
    const loading = document.getElementById('loading');
    const tableContainer = document.querySelector('.table-container');
    
    if (show) {
        loading.style.display = 'flex';
        tableContainer.style.opacity = '0.5';
    } else {
        loading.style.display = 'none';
        tableContainer.style.opacity = '1';
    }
}

// Format date
function formatDate(dateString) {
    try {
        const date = new Date(dateString);
        return date.toLocaleDateString('pt-BR');
    } catch (error) {
        return dateString;
    }
}

// Refresh data
function refreshData() {
    loadAgentsData();
}

// Add refresh button functionality
document.addEventListener('DOMContentLoaded', function() {
    // Add refresh button to header
    const headerRight = document.querySelector('.header-right');
    const refreshBtn = document.createElement('button');
    refreshBtn.className = 'btn btn-secondary';
    refreshBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Refresh';
    refreshBtn.addEventListener('click', refreshData);
    headerRight.insertBefore(refreshBtn, headerRight.firstChild);
    
    // Load initial data
    loadAgentsData();
    
    // Initialize filter options
    initializeFilterOptions();
    
    // Close dropdowns when clicking outside
    document.addEventListener('click', function(e) {
        if (!e.target.closest('.filter-dropdown') && !e.target.closest('.filter-icon')) {
            closeAllFilters();
        }
    });
});

// Initialize filter options for each column
function initializeFilterOptions() {
    const columns = ['agent', 'ufg', 'preLicenseEnrollment', 'licensed', 'preLicensePercent', 
                    'enrollmentDate', 'finishDate', 'timeSpent', 'lastLogin', 'courseName', 
                    'preparedToPass', 'phone'];
    
    columns.forEach(column => {
        activeFilters[column] = new Set();
        filterOptions[column] = new Set();
    });
    
    // Populate filter options from data
    agentsData.forEach(agent => {
        Object.keys(filterOptions).forEach(column => {
            if (agent[column] !== undefined && agent[column] !== null && agent[column] !== '') {
                filterOptions[column].add(agent[column]);
            }
        });
    });
    
    // Render filter options
    renderFilterOptions();
}

// Render filter options for all columns
function renderFilterOptions() {
    Object.keys(filterOptions).forEach(column => {
        const optionsContainer = document.getElementById(`${column}-options`);
        if (!optionsContainer) return;
        
        const options = Array.from(filterOptions[column]).sort();
        
        optionsContainer.innerHTML = `
            <div class="filter-option select-all" onclick="toggleSelectAll('${column}')">
                <input type="checkbox" id="select-all-${column}">
                <span class="option-label">(Select All)</span>
            </div>
            ${options.map(option => `
                <div class="filter-option" onclick="toggleFilterOption('${column}', '${option}')">
                    <input type="checkbox" id="${column}-${option.replace(/[^a-zA-Z0-9]/g, '_')}" 
                           ${activeFilters[column].has(option) ? 'checked' : ''}>
                    <span class="option-label">${option}</span>
                </div>
            `).join('')}
        `;
    });
}

// Toggle filter dropdown
function toggleFilter(column) {
    // Close all other filters
    closeAllFilters();
    
    // Toggle current filter
    const dropdown = document.getElementById(`filter-${column}`);
    if (dropdown) {
        dropdown.style.display = dropdown.style.display === 'none' ? 'block' : 'none';
        
        // Update filter icon
        const icon = document.querySelector(`[onclick="toggleFilter('${column}')"]`);
        if (icon) {
            icon.classList.toggle('active', dropdown.style.display === 'block');
        }
    }
}

// Update filter icon state based on active filters
function updateFilterIconStates() {
    // Update Agent filter icon
    const agentIcon = document.querySelector(`[onclick="toggleFilter('agent')"]`);
    if (agentIcon) {
        agentIcon.classList.toggle('active', agentAdvancedFilter.active);
    }
    
    // Update other filter icons
    Object.keys(activeFilters).forEach(column => {
        if (column !== 'agent') {
            const icon = document.querySelector(`[onclick="toggleFilter('${column}')"]`);
            if (icon) {
                icon.classList.toggle('active', activeFilters[column].size > 0);
            }
        }
    });
}

// Close all filter dropdowns
function closeAllFilters() {
    document.querySelectorAll('.filter-dropdown').forEach(dropdown => {
        dropdown.style.display = 'none';
    });
    
    // Remove active class from all filter icons
    document.querySelectorAll('.filter-icon').forEach(icon => {
        icon.classList.remove('active');
    });
}

// Toggle select all for a column
function toggleSelectAll(column) {
    const selectAllCheckbox = document.getElementById(`select-all-${column}`);
    const isChecked = selectAllCheckbox.checked;
    
    // Update all individual checkboxes
    const options = Array.from(filterOptions[column]);
    options.forEach(option => {
        const checkbox = document.getElementById(`${column}-${option.replace(/[^a-zA-Z0-9]/g, '_')}`);
        if (checkbox) {
            checkbox.checked = isChecked;
        }
        
        if (isChecked) {
            activeFilters[column].add(option);
        } else {
            activeFilters[column].delete(option);
        }
    });
    
    applyAdvancedFilters();
}

// Toggle individual filter option
function toggleFilterOption(column, option) {
    const checkbox = document.getElementById(`${column}-${option.replace(/[^a-zA-Z0-9]/g, '_')}`);
    const isChecked = checkbox.checked;
    
    if (isChecked) {
        activeFilters[column].add(option);
    } else {
        activeFilters[column].delete(option);
    }
    
    // Update select all checkbox
    const selectAllCheckbox = document.getElementById(`select-all-${column}`);
    const totalOptions = filterOptions[column].size;
    const selectedOptions = activeFilters[column].size;
    
    if (selectedOptions === 0) {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
    } else if (selectedOptions === totalOptions) {
        selectAllCheckbox.checked = true;
        selectAllCheckbox.indeterminate = false;
    } else {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = true;
    }
    
    applyAdvancedFilters();
}

// Apply advanced filters
function applyAdvancedFilters() {
    filteredData = agentsData.filter(agent => {
        // Apply advanced Agent filter first
        if (agentAdvancedFilter.active) {
            const agentPasses = filterAgent(agent, agentAdvancedFilter.condition, agentAdvancedFilter.value);
            if (!agentPasses) {
                return false;
            }
        }
        
        // Apply other column filters
        return Object.keys(activeFilters).every(column => {
            const columnFilters = activeFilters[column];
            
            // If no filters are active for this column, include all
            if (columnFilters.size === 0) {
                return true;
            }
            
            // Check if agent's value for this column is in the active filters
            const agentValue = agent[column];
            return columnFilters.has(agentValue);
        });
    });
    
    renderTable();
    updateCounts();
}

// Update the old applyFilters function to work with new system
function applyFilters() {
    // This function is kept for backward compatibility
    // The new system uses applyAdvancedFilters()
    applyAdvancedFilters();
}

// Advanced Agent Filter Functions
function updateAgentFilter() {
    const condition = document.getElementById('agent-condition').value;
    const value = document.getElementById('search-agent-filter').value;
    
    agentAdvancedFilter.condition = condition;
    agentAdvancedFilter.value = value;
    
    // Hide search input for blank/not blank conditions
    const searchInput = document.querySelector('#filter-agent .filter-search');
    if (condition === 'blank' || condition === 'not_blank') {
        searchInput.style.display = 'none';
    } else {
        searchInput.style.display = 'block';
    }
}

function applyAgentFilter() {
    const condition = document.getElementById('agent-condition').value;
    const value = document.getElementById('search-agent-filter').value;
    
    agentAdvancedFilter.condition = condition;
    agentAdvancedFilter.value = value;
    agentAdvancedFilter.active = true;
    
    // Close the filter dropdown
    closeAllFilters();
    
    // Apply the filter
    applyAdvancedFilters();
    
    // Update filter icon states
    updateFilterIconStates();
}

function clearAgentFilter() {
    agentAdvancedFilter.condition = 'contains';
    agentAdvancedFilter.value = '';
    agentAdvancedFilter.active = false;
    
    // Reset the UI
    document.getElementById('agent-condition').value = 'contains';
    document.getElementById('search-agent-filter').value = '';
    
    // Close the filter dropdown
    closeAllFilters();
    
    // Apply the filter
    applyAdvancedFilters();
    
    // Update filter icon states
    updateFilterIconStates();
}

// Enhanced filter function for Agent column
function filterAgent(agent, condition, value) {
    const agentName = agent.name.toLowerCase();
    const searchValue = value.toLowerCase();
    
    switch (condition) {
        case 'contains':
            return agentName.includes(searchValue);
        case 'not_contains':
            return !agentName.includes(searchValue);
        case 'equals':
            return agentName === searchValue;
        case 'not_equals':
            return agentName !== searchValue;
        case 'starts_with':
            return agentName.startsWith(searchValue);
        case 'ends_with':
            return agentName.endsWith(searchValue);
        case 'blank':
            return !agentName || agentName.trim() === '';
        case 'not_blank':
            return agentName && agentName.trim() !== '';
        default:
            return true;
    }
}

// Context menu functions
function toggleContextMenu(column, event) {
    event.stopPropagation();
    
    // Close all other context menus
    document.querySelectorAll('.context-menu').forEach(menu => {
        if (menu.id !== `context-menu-${column}`) {
            menu.style.display = 'none';
        }
    });
    
    // Toggle current context menu
    const menu = document.getElementById(`context-menu-${column}`);
    if (menu.style.display === 'none' || menu.style.display === '') {
        menu.style.display = 'block';
    } else {
        menu.style.display = 'none';
    }
}

function sortColumnFromMenu(column, direction) {
    sortColumn = column;
    sortDirection = direction;
    sortTable(column);
    closeAllContextMenus();
}

function togglePinColumn(column) {
    if (pinnedColumns.has(column)) {
        pinnedColumns.delete(column);
    } else {
        pinnedColumns.add(column);
    }
    updateColumnOrder();
    closeAllContextMenus();
}

function autosizeColumn(column) {
    const table = document.getElementById('agents-table');
    const columnIndex = getColumnIndex(column);
    const th = table.querySelectorAll('th')[columnIndex];
    const tbody = table.querySelector('tbody');
    
    if (!th || !tbody) return;
    
    // Calculate max width needed
    let maxWidth = th.offsetWidth;
    
    // Check header width
    const headerText = th.querySelector('span').textContent;
    const headerWidth = getTextWidth(headerText, '14px Arial') + 60; // Add padding and icons
    maxWidth = Math.max(maxWidth, headerWidth);
    
    // Check data cell widths
    const cells = tbody.querySelectorAll(`td:nth-child(${columnIndex + 1})`);
    cells.forEach(cell => {
        const cellWidth = getTextWidth(cell.textContent, '14px Arial') + 32; // Add padding
        maxWidth = Math.max(maxWidth, cellWidth);
    });
    
    // Set minimum and maximum widths
    maxWidth = Math.max(120, Math.min(maxWidth, 300));
    th.style.minWidth = `${maxWidth}px`;
    th.style.width = `${maxWidth}px`;
    
    closeAllContextMenus();
}

function autosizeAllColumns() {
    const columns = ['agent', 'ufg', 'preLicenseEnrollment', 'licensed', 'preLicensePercent', 
                    'enrollmentDate', 'finishDate', 'timeSpent', 'lastLogin', 'courseName', 
                    'preparedToPass', 'phone'];
    
    columns.forEach(column => {
        autosizeColumn(column);
    });
    
    closeAllContextMenus();
}

function chooseColumns() {
    // Create column visibility modal
    showColumnVisibilityModal();
    closeAllContextMenus();
}

function resetColumns() {
    // Reset all column settings
    pinnedColumns.clear();
    columnVisibility = {};
    
    // Reset column order
    updateColumnOrder();
    
    // Reset column widths
    document.querySelectorAll('th').forEach(th => {
        th.style.minWidth = '';
        th.style.width = '';
    });
    
    closeAllContextMenus();
}

function closeAllContextMenus() {
    document.querySelectorAll('.context-menu').forEach(menu => {
        menu.style.display = 'none';
    });
}

function getColumnIndex(column) {
    const columns = ['agent', 'ufg', 'preLicenseEnrollment', 'licensed', 'preLicensePercent', 
                    'enrollmentDate', 'finishDate', 'timeSpent', 'lastLogin', 'courseName', 
                    'preparedToPass', 'phone'];
    return columns.indexOf(column);
}

function getTextWidth(text, font) {
    const canvas = document.createElement('canvas');
    const context = canvas.getContext('2d');
    context.font = font;
    return context.measureText(text).width;
}

function updateColumnOrder() {
    const table = document.getElementById('agents-table');
    const thead = table.querySelector('thead tr');
    const tbody = table.querySelector('tbody');
    
    if (!thead || !tbody) return;
    
    // Get all column headers
    const headers = Array.from(thead.querySelectorAll('th'));
    
    // Sort headers: pinned first, then others
    const sortedHeaders = headers.sort((a, b) => {
        const aColumn = a.getAttribute('data-column');
        const bColumn = b.getAttribute('data-column');
        
        const aPinned = pinnedColumns.has(aColumn);
        const bPinned = pinnedColumns.has(bColumn);
        
        if (aPinned && !bPinned) return -1;
        if (!aPinned && bPinned) return 1;
        return 0;
    });
    
    // Reorder headers
    sortedHeaders.forEach(header => {
        thead.appendChild(header);
    });
    
    // Reorder data cells
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.forEach(row => {
        const cells = Array.from(row.querySelectorAll('td'));
        const sortedCells = sortedHeaders.map(header => {
            const columnIndex = getColumnIndex(header.getAttribute('data-column'));
            return cells[columnIndex];
        });
        
        sortedCells.forEach(cell => {
            row.appendChild(cell);
        });
    });
}

function showColumnVisibilityModal() {
    // Create modal for column visibility
    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>Choose Columns</h3>
                <button class="close-btn" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div class="column-list">
                    <label><input type="checkbox" checked> Agent</label>
                    <label><input type="checkbox" checked> UFG</label>
                    <label><input type="checkbox" checked> Pre-License Enrollment</label>
                    <label><input type="checkbox" checked> Licensed</label>
                    <label><input type="checkbox" checked> Pre-License %</label>
                    <label><input type="checkbox" checked> Enrollment Date</label>
                    <label><input type="checkbox" checked> Finish Date</label>
                    <label><input type="checkbox" checked> Time Spent</label>
                    <label><input type="checkbox" checked> Last Login</label>
                    <label><input type="checkbox" checked> Course Name</label>
                    <label><input type="checkbox" checked> Prepared To Pass</label>
                    <label><input type="checkbox" checked> Phone</label>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal()">Cancel</button>
                <button class="btn btn-primary" onclick="applyColumnVisibility()">Apply</button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
}

function closeModal() {
    const modal = document.querySelector('.modal');
    if (modal) {
        modal.remove();
    }
}

function applyColumnVisibility() {
    // Apply column visibility changes
    closeModal();
}

// Close context menus when clicking outside
document.addEventListener('click', function(event) {
    if (!event.target.closest('.context-menu') && !event.target.closest('.menu-icon')) {
        closeAllContextMenus();
    }
});
