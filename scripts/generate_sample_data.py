import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def check_directory():
    """Check if data/raw directory exists"""
    raw_dir = Path('data/raw')
    if not raw_dir.exists():
        raise FileNotFoundError("data/raw directory not found. Please ensure it exists before running this script.")
    logger.info("Found data/raw directory")

def generate_dev_backlog():
    """Generate development backlog items"""
    logger.info("Generating development backlog...")
    features = [
        ("Email Integration with Gmail API", "Implement two-way email sync with Gmail, including contact import and email tracking", "Backend"),
        ("Custom Dashboard Builder", "Allow users to create custom dashboards with drag-and-drop widgets, saved layouts, and data filtering", "Frontend"),
        ("Bulk Contact Import Enhancement", "Add support for custom field mapping, duplicate detection, and error handling in bulk imports", "Backend"),
        ("Mobile App Offline Mode", "Implement offline data access and sync for core CRM functions in mobile app", "Mobile"),
        ("Advanced Search Filters", "Add advanced search capabilities including saved searches, boolean operators, and custom field search", "Frontend"),
        ("API Rate Limiting Enhancement", "Implement smart rate limiting and usage monitoring for API endpoints", "Backend"),
        ("Custom Field Type Expansion", "Add support for new field types including formula fields and lookups", "Backend"),
        ("Report Builder 2.0", "Enhanced reporting with custom formulas, pivot tables, and scheduled exports", "Analytics"),
        ("Activity Timeline Improvements", "Add filters and better visualization for contact activity timeline", "Frontend"),
        ("Data Import Wizard", "New wizard interface for data imports with preview and mapping", "Frontend"),
        ("Mobile Push Notifications", "Implement customizable push notifications for mobile app", "Mobile"),
        ("Calendar Integration Enhancement", "Improve two-way sync with Google and Outlook calendars", "Backend"),
        ("Custom Workflow Builder", "Visual workflow builder for automation rules", "Backend"),
        ("Contact Merge Tool", "Enhanced tool for merging duplicate contacts with preview", "Frontend"),
        ("Email Template Designer", "Drag-and-drop email template designer with dynamic fields", "Frontend"),
        ("Performance Optimization Phase 1", "Optimize database queries and cache implementation", "Backend"),
        ("Social Media Integration", "Add social media profile linking and activity tracking", "Backend"),
        ("Document Management System", "Implement document storage, versioning, and sharing", "Backend"),
        ("Mobile UI Refresh", "Update mobile app UI to match new design system", "Mobile"),
        ("Advanced Permission System", "Granular permission controls for teams and roles", "Backend")
    ]
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i, (title, desc, team) in enumerate(features, 1):
        created_date = start_date + timedelta(days=random.randint(0, 45))
        target_date = created_date + timedelta(days=random.randint(30, 120))
        
        data.append({
            'ticket_id': f'DEV-{i:03d}',
            'title': title,
            'description': desc,
            'status': random.choice(['Planned'] * 4 + ['In Progress']),
            'priority': random.choice(['High'] * 3 + ['Medium'] * 4 + ['Low'] * 2),
            'created_date': created_date.strftime('%Y-%m-%d'),
            'target_release_date': target_date.strftime('%Y-%m-%d'),
            'story_points': random.choice([5, 8, 13, 21, 34]),
            'assigned_team': team
        })
    
    return pd.DataFrame(data)  # Make sure we return the DataFrame

def generate_csat_data(quarter: int, year: int = 2023):
    """Generate CSAT survey responses for a specific quarter"""
    logger.info(f"Generating CSAT data for Q{quarter} {year}...")
    feedback_themes = [
        "Email integration", "Mobile app", "Dashboard", "Search functionality",
        "Performance", "Customer support", "API", "Data import/export",
        "Calendar sync", "Workflow automation", "UI/UX", "Reporting"
    ]
    
    quarter_start = datetime(year, 1 + (quarter-1)*3, 1)
    quarter_end = datetime(year, 3 + (quarter-1)*3, 28)
    
    data = []
    for i in range(15):  # 15 responses per quarter
        survey_date = quarter_start + timedelta(days=random.randint(0, (quarter_end - quarter_start).days))
        satisfaction = random.choices([2,3,4,5], weights=[1,3,4,2])[0]
        
        theme = random.choice(feedback_themes)
        print(f"Generated theme for response {i+1}: {theme}")  # Print the selected theme
        
        if satisfaction >= 4:
            reason = random.choice([
                f"Great {theme} functionality",
                f"Recent improvements to {theme}",
                "Excellent customer support",
                "System reliability",
                "Easy to use interface"
            ])
        else:
            reason = random.choice([
                f"{theme} needs improvement",
                f"Issues with {theme}",
                "Performance problems",
                "Missing features",
                "Integration issues"
            ])
        
        feedback_text = f"{reason} - {theme} feedback."
        
        data.append({
            'response_id': f'CS{quarter}{i+1:02d}',
            'survey_date': survey_date.strftime('%Y-%m-%d'),
            'satisfaction_score': satisfaction,
            'reason_for_rating': reason,
            'feature_feedback': f"Comments about {theme}",
            'improvement_suggestions': f"Suggestions for {theme}",
            'feedback_text': feedback_text
        })
    
    return pd.DataFrame(data)  # Make sure we return the DataFrame

def generate_support_tickets():
    """Generate support ticket data"""
    logger.info("Generating support tickets...")
    categories = ['Technical', 'Feature Request', 'Bug', 'Usage', 'Integration']
    issues = [
        "Cannot access dashboard",
        "Email sync not working",
        "Mobile app crashes",
        "Search not returning results",
        "Need help with API integration",
        "Calendar sync issues",
        "Data import failed",
        "Custom field problems",
        "Report generation error",
        "Workflow automation issue"
    ]
    
    data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(25):
        created_date = start_date + timedelta(days=random.randint(0, 365))
        category = random.choice(categories)
        issue = random.choice(issues)
        
        data.append({
            'ticket_id': f'SUP-{i+1:03d}',
            'created_date': created_date.strftime('%Y-%m-%d'),
            'status': 'Closed',
            'priority': random.choice(['High', 'Medium', 'Low']),
            'category': category,
            'subject': issue,
            'description': f"Customer reported: {issue}",
            'resolution_notes': f"Resolution for {issue}"
        })
    
    return pd.DataFrame(data)  # Make sure we return the DataFrame

def main():
    """Generate all sample data files"""
    try:
        # Verify directory exists
        check_directory()
        
        # Generate and save development backlog
        dev_backlog = generate_dev_backlog()
        output_path = 'data/raw/dev_backlog.csv'
        dev_backlog.to_csv(output_path, index=False)
        logger.info(f"Saved development backlog to {output_path}")
        
        # Generate and save CSAT data for each quarter
        for quarter in range(1, 5):
            csat_data = generate_csat_data(quarter)
            output_path = f'data/raw/csat_2023_q{quarter}.csv'
            csat_data.to_csv(output_path, index=False)
            logger.info(f"Saved Q{quarter} CSAT data to {output_path}")
        
        # Generate and save support tickets
        support_tickets = generate_support_tickets()
        output_path = 'data/raw/support_tickets.csv'
        support_tickets.to_csv(output_path, index=False)
        logger.info(f"Saved support tickets to {output_path}")
        
        logger.info("Sample data generation complete!")
        
    except Exception as e:
        logger.error(f"Error generating sample data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 