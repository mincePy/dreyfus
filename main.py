import logging
from pathlib import Path
from src.preprocessing import DataPreprocessor
from src.data_ingestion import DataIngestion
from src.sentiment_analysis import TextAnalyzer
from src.impact_analysis import ImpactAnalyzer
from src.visualise import DevelopmentVisualizer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline():
    """Run the complete analysis pipeline"""
    try:
        logger.info("Starting analysis pipeline...")
        
        # 1. Preprocess raw data
        logger.info("Preprocessing data...")
        preprocessor = DataPreprocessor()
        preprocessor.process_all()
        
        # 2. Load processed data
        logger.info("Loading data...")
        ingestion = DataIngestion()
        csat_df, tickets_df, dev_tickets_df = ingestion.get_combined_data()
        
        # 3. Run sentiment and theme analysis
        logger.info("Running text analysis...")
        analyzer = TextAnalyzer()
        analysis_results = analyzer.analyze_all(csat_df, tickets_df)
        
        # 4. Perform impact analysis
        logger.info("Analyzing impact...")
        impact_analyzer = ImpactAnalyzer()
        impact_metrics = impact_analyzer.analyze_impact(dev_tickets_df['target_release_date'].min())
        
        # 5. Generate visualizations
        logger.info("Creating visualizations...")
        visualizer = DevelopmentVisualizer()
        visualizer.create_visualizations()
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline() 