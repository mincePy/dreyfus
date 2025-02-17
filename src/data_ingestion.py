import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Optional

# Data ingestion module for loading and processing customer satisfaction (CSAT) surveys
# and support ticket data.
#
# Usage:
#   from data_ingestion import DataIngestion
#   
#   # Initialize - will automatically find files in data/output directory
#   ingestion = DataIngestion()
#
#   # Or initialize with specific paths (optional)
#   ingestion = DataIngestion(
#       csat_path='path/to/csat_surveys.csv',
#       tickets_path='path/to/support_tickets.csv'
#   )
#
#   # Load and process the data
#   csat_df = ingestion.load_csat_data()
#   tickets_df = ingestion.load_support_tickets()
#
# Input file requirements:
#   - CSAT CSV file should contain: satisfaction_score, survey_date
#   - Support tickets file format TBD
#

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataIngestion:
    def __init__(self, csat_path: str = None, tickets_path: str = None, dev_tickets_path: str = None, auto_load: bool = True):
        """Initialize data ingestion with paths to CSAT, support ticket, and dev ticket data.
        If no paths provided, will look in data/output directory for processed files
        and data/raw for dev tickets.
        
        Args:
            csat_path (str, optional): Path to CSAT survey CSV file
            tickets_path (str, optional): Path to support tickets data file
            dev_tickets_path (str, optional): Path to development backlog tickets
            auto_load (bool): Whether to load data automatically upon initialization
        """
        output_dir = Path('data/output')
        raw_dir = Path('data/raw')
        
        # If no paths provided, look for files in data/output
        if not csat_path:
            csat_files = list(output_dir.glob('*csat*.csv'))
            self.csat_path = csat_files[0] if csat_files else None
        else:
            self.csat_path = Path(csat_path)
            
        if not tickets_path:
            ticket_files = list(output_dir.glob('*ticket*.csv'))
            self.tickets_path = ticket_files[0] if ticket_files else None
        else:
            self.tickets_path = Path(tickets_path)
            
        # Look for dev tickets in raw directory
        if not dev_tickets_path:
            dev_ticket_files = list(raw_dir.glob('*dev*backlog*.csv'))
            self.dev_tickets_path = dev_ticket_files[0] if dev_ticket_files else None
        else:
            self.dev_tickets_path = Path(dev_tickets_path)
            
        self.csat_data = None
        self.tickets_data = None
        self.dev_tickets_data = None
        
        if auto_load:
            self.load_all_data()
    
    def load_all_data(self) -> tuple:
        """Load all available data sources
        
        Returns:
            tuple: (csat_df, tickets_df, dev_tickets_df)
        """
        return (
            self.load_csat_data(),
            self.load_support_tickets(),
            self.load_dev_tickets()
        )
    
    def load_csat_data(self) -> Optional[pd.DataFrame]:
        """Load CSAT survey data"""
        if not self.csat_path or not self.csat_path.exists():
            logger.warning("No CSAT data file found")
            return None
            
        logger.info(f"Loading CSAT data from {self.csat_path}")
        self.csat_data = pd.read_csv(self.csat_path)
        return self.csat_data
    
    def load_support_tickets(self) -> Optional[pd.DataFrame]:
        """Load support ticket data"""
        if not self.tickets_path or not self.tickets_path.exists():
            logger.warning("No support tickets file found")
            return None
            
        logger.info(f"Loading support tickets from {self.tickets_path}")
        self.tickets_data = pd.read_csv(self.tickets_path)
        return self.tickets_data
    
    def load_dev_tickets(self) -> Optional[pd.DataFrame]:
        """Load development backlog tickets
        
        Expected columns:
            - ticket_id: unique identifier
            - title: ticket title/summary
            - description: detailed description
            - status: current status
            - priority: ticket priority
            - created_date: when ticket was created
            - target_release_date: planned release date
        """
        if not self.dev_tickets_path or not self.dev_tickets_path.exists():
            logger.warning("No development tickets file found")
            return None
            
        logger.info(f"Loading development tickets from {self.dev_tickets_path}")
        self.dev_tickets_data = pd.read_csv(self.dev_tickets_path)
        
        # Validate required columns
        required_cols = [
            'ticket_id', 'title', 'description', 'status',
            'priority', 'created_date', 'target_release_date'
        ]
        
        missing_cols = [col for col in required_cols if col not in self.dev_tickets_data.columns]
        if missing_cols:
            logger.warning(f"Development tickets missing columns: {missing_cols}")
        
        # Convert dates to datetime
        date_columns = ['created_date', 'target_release_date']
        for col in date_columns:
            if col in self.dev_tickets_data.columns:
                self.dev_tickets_data[col] = pd.to_datetime(self.dev_tickets_data[col])
        
        return self.dev_tickets_data

    def get_combined_data(self) -> tuple:
        """Load and return all data sources
        
        Returns:
            tuple: (csat_df, tickets_df, dev_tickets_df)
        """
        if self.csat_data is None or self.tickets_data is None or self.dev_tickets_data is None:
            return self.load_all_data()
        return self.csat_data, self.tickets_data, self.dev_tickets_data
