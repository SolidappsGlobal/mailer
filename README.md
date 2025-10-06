# Agents Dashboard - Pre-Licensing

A modern web interface for viewing and managing pre-licensing agent data.

## 🚀 Features

### 📊 Data Table
- **Complete visualization** of agents with detailed information
- **Responsive design** that adapts to different screen sizes
- **Sorting** by any column (click on header)
- **Multiple selection** of agents with checkboxes

### 🔍 Advanced Filters
- **Column filters** - Each column has its own filter dropdown
- **Search within filter** - Search field in each dropdown
- **Select All/Deselect All** - Select/deselect all options
- **Individual checkboxes** - Granular control of each value
- **Multiple filters** - Combine filters from different columns
- **Clear filters** - Button to clear all filters at once

### 📤 Export
- **Export CSV** of selected data or all data
- **Complete data** including email, phone and manager

### 🔄 API Integration
- **Bubble API** - Loads real data from the system
- **Back4App** - Fallback for alternative data
- **Automatic data update**

## 📁 File Structure

```
services_xcel-to-bubble_1755211990.284000/
├── index.html          # Main page structure
├── styles.css          # Responsive styles and design
├── script.js           # JavaScript logic and API integration
├── main.py            # Python server (Cloud Run)
└── README.md          # This file
```

## 🛠️ How to Use

### 1. Open the Application
```bash
# Navigate to the project folder
cd services_xcel-to-bubble_1755211990.284000

# Open the index.html file in a browser
# Or use a local server:
python -m http.server 8000
# Access: http://localhost:8000
```

### 2. Main Features

#### **View Data**
- The table automatically loads agent data
- Use horizontal scroll bar on smaller screens
- Click on headers to sort

#### **Filter and Search**
- Click on filter icons in column headers to open dropdowns
- Use search fields within each dropdown
- Click "Clear Filters" to clear all filters

#### **Select Agents**
- Check the checkboxes to select specific agents
- The "Selected" counter shows how many are selected
- If none are selected, export includes all data

#### **Export Data**
- Click "Export CSV" to download the data
- The file will be saved with the current date
- Includes all available fields

#### **Update Data**
- Click "Refresh" to reload data from APIs
- Data is automatically loaded when opening the page

## 🔧 Configuration

### Configured APIs
```javascript
const CONFIG = {
    BUBBLE_API_URL: 'https://your-bubble-app.com/api/1.1/obj/your-table',
    BUBBLE_TOKEN: 'your-bubble-token-here',
    BACK4APP_API_URL: 'https://your-back4app-app.b4a.run',
    BACK4APP_TOKEN: 'your-back4app-token-here'
};
```

### Environment Setup
To configure the application with your own APIs:

1. **Bubble API Setup**:
   - Create a Bubble app
   - Set up your data table
   - Get your API token from Bubble settings
   - Update `BUBBLE_API_URL` and `BUBBLE_TOKEN` in `script.js`

2. **Back4App API Setup** (Optional):
   - Create a Back4App account
   - Set up your database
   - Get your App ID and Master Key
   - Update `BACK4APP_API_URL` and `BACK4APP_TOKEN` in `script.js`

3. **Security Note**:
   - Never commit real API keys to version control
   - Use environment variables for production
   - Consider using a backend proxy for sensitive operations

### Table Fields
- **Agent**: Agent name and role
- **UFG**: Unique identifier code
- **Pre-License Enrollment**: Enrollment status
- **Licensed**: Licensing status
- **Pre-License %**: Completion percentage
- **Enrollment Date**: Enrollment date
- **Finish Date**: Completion date
- **Time Spent**: Time spent in course
- **Last Login**: Last login date
- **Course Name**: Course name
- **Prepared To Pass**: Preparation status
- **Phone**: Phone number

## 🎨 Design

### Visual Characteristics
- **Clean and modern** interface
- **Soft colors** focused on usability
- **Font Awesome icons** for better UX
- **Smooth animations** for interactions
- **Responsive** for mobile and desktop

### Color Palette
- **Primary**: #007bff (Blue)
- **Secondary**: #6c757d (Gray)
- **Success**: #d4edda (Light green)
- **Error**: #f8d7da (Light red)
- **Background**: #f8f9fa (Very light gray)

## 🔄 Backend Integration

### Data Flow
1. **Frontend** loads data from Bubble API
2. **Fallback** to sample data if API fails
3. **Local processing** for filters and sorting
4. **Export** generates CSV in browser

### Supported APIs
- **Bubble API**: Primary data source
- **Back4App**: Alternative source (configurable)
- **Sample data**: For demonstration

## 🚀 Deploy

### Local Server
```bash
python -m http.server 8000
```

### Web Server
- Upload files to any web server
- No specific backend required
- Works with Apache, Nginx, etc.

### Cloud Run (with Python backend)
- Use `main.py` to process data
- Frontend can be served statically
- APIs configured for production

## 📱 Responsiveness

### Breakpoints
- **Desktop**: > 1200px
- **Tablet**: 768px - 1200px
- **Mobile**: < 768px

### Mobile Adaptations
- Table with horizontal scroll
- Filters in single column
- Stacked buttons
- Touch-optimized text

## 🐛 Troubleshooting

### Common Issues
1. **Data doesn't load**: Check API connection
2. **Filters don't work**: Clear browser cache
3. **Export fails**: Check if there's selected data
4. **Broken layout**: Use a modern browser (Chrome, Firefox, Safari)

### Debug Logs
- Open browser Console (F12)
- Check network errors in Network/Console tabs
- Sample data is automatically loaded in case of error

## 📈 Upcoming Features

- [ ] Pagination for large data volumes
- [ ] Inline field editing
- [ ] Charts and reports
- [ ] Real-time notifications
- [ ] User authentication
- [ ] Change history

## 📊 Logging and Monitoring

### Log Configuration
The application includes comprehensive logging for monitoring and debugging:

#### **Log Levels**
- **INFO**: General application flow and requests
- **WARNING**: Non-critical issues (e.g., missing Cloud Logging)
- **ERROR**: Critical errors and exceptions

#### **Log Output**
- **Console**: All logs are written to stdout for Cloud Run
- **Google Cloud Logging**: Automatically integrated when available
- **Structured Format**: Timestamp, logger name, level, and message

#### **What Gets Logged**
- **Application startup** with configuration details
- **HTTP requests** (method, URL, IP address)
- **HTTP responses** (status codes)
- **CSV processing** (URL, row counts, progress)
- **API calls** (Bubble and Back4App operations)
- **Errors and exceptions** with full stack traces

#### **Viewing Logs**
```bash
# Cloud Run logs
gcloud logs read --service=your-service-name --limit=50

# Real-time logs
gcloud logs tail --service=your-service-name

# Filter by severity
gcloud logs read --service=your-service-name --severity=ERROR
```

#### **Log Files**
- `logging.conf`: Configuration file for structured logging
- Automatically included in Docker container
- No additional setup required

## 🔒 Security

### API Keys Protection
- **Never commit** real API keys to version control
- **Use environment variables** for production deployments
- **Rotate keys regularly** for enhanced security
- **Implement rate limiting** on your APIs
- **Use HTTPS** for all API communications

### Best Practices
- Store sensitive configuration in environment variables
- Use a backend proxy for API calls in production
- Implement proper authentication and authorization
- Monitor API usage and set up alerts
- Regular security audits of your API endpoints

### Development vs Production
- **Development**: Use sample data or test APIs
- **Production**: Use environment variables and secure endpoints
- **Staging**: Use separate API keys for testing