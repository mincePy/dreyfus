from pathlib import Path
import pandas as pd
import logging
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        self.raw_dir = Path('data/raw')
        self.output_dir = Path('data/output')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def process_all(self):
        """Process both CSAT and support ticket data"""
        self.process_csat_data()
        self.process_ticket_data()
    
    def process_csat_data(self):
        """Process all CSAT files from raw directory and output a single cleaned file"""
        # Find all CSAT files
        csat_files = list(self.raw_dir.glob('*csat*.csv'))
        if not csat_files:
            logger.warning("No CSAT files found in raw directory")
            return
        
        # Read and combine all CSAT files
        dfs = []
        for file in csat_files:
            logger.info(f"Processing CSAT file: {file.name}")
            try:
                df = pd.read_csv(file)
                df = self._clean_csat_data(df)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing {file.name}: {str(e)}")
                continue
        
        if not dfs:
            logger.error("No valid CSAT data to process")
            return
            
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates
        combined_df = combined_df.drop_duplicates()
        
        # Save to output
        output_path = self.output_dir / 'csat_processed.csv'
        combined_df.to_csv(output_path, index=False)
        logger.info(f"Saved processed CSAT data to {output_path}")
    
    def process_ticket_data(self):
        """Process all support ticket files from raw directory and output a single cleaned file"""
        # Find all ticket files
        ticket_files = list(self.raw_dir.glob('*ticket*.csv'))
        if not ticket_files:
            logger.warning("No ticket files found in raw directory")
            return
        
        # Read and combine all ticket files
        dfs = []
        for file in ticket_files:
            logger.info(f"Processing ticket file: {file.name}")
            try:
                df = pd.read_csv(file)
                df = self._clean_ticket_data(df)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing {file.name}: {str(e)}")
                continue
        
        if not dfs:
            logger.error("No valid ticket data to process")
            return
            
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates
        combined_df = combined_df.drop_duplicates()
        
        # Save to output
        output_path = self.output_dir / 'tickets_processed.csv'
        combined_df.to_csv(output_path, index=False)
        logger.info(f"Saved processed ticket data to {output_path}")
    
    def _clean_csat_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate CSAT data
        
        Expected columns: satisfaction_score, survey_date, [other columns TBD]
        """
        # Verify required columns
        required_cols = ['satisfaction_score', 'survey_date']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Missing required columns. Expected: {required_cols}")
        
        # Convert dates to datetime
        df['survey_date'] = pd.to_datetime(df['survey_date'])
        
        # Validate satisfaction scores (assuming 1-5 scale)
        if not df['satisfaction_score'].between(1, 5).all():
            logger.warning("Found satisfaction scores outside expected range (1-5)")
            df = df[df['satisfaction_score'].between(1, 5)]
        
        return df
    
    def _clean_ticket_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate support ticket data
        
        Expected columns: 
            - ticket_id: unique identifier
            - created_date: when ticket was created
            - status: ticket status (e.g., 'open', 'closed')
            - category: type of support ticket
        """
        # Verify required columns
        required_cols = ['ticket_id', 'created_date', 'status', 'category']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Missing required columns. Expected: {required_cols}")
        
        # Basic cleaning steps
        df = df.copy()
        
        # Convert dates to datetime
        df['created_date'] = pd.to_datetime(df['created_date'])
        
        # Convert to lowercase and strip whitespace from string columns
        for col in ['status', 'category']:
            df[col] = df[col].str.lower().str.strip()
        
        # Remove duplicates based on ticket_id
        df = df.drop_duplicates(subset=['ticket_id'])
        
        # Sort by created_date
        df = df.sort_values('created_date')
        
        return df

if __name__ == "__main__":
    preprocessor = DataPreprocessor()
    preprocessor.process_all()
