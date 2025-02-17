import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, List, NamedTuple
import logging
from pathlib import Path
from src.data_ingestion import DataIngestion
from src.sentiment_analysis import TextAnalyzer
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ImpactMetrics(NamedTuple):
    """Container for impact analysis results"""
    sentiment_change: float  # Change in average sentiment
    theme_changes: Dict[str, float]  # Changes in theme prevalence
    significant_themes: List[str]  # Themes with significant changes
    p_value: float  # Statistical significance of sentiment change

class ImpactAnalyzer:
    def __init__(self):
        self.data_ingestion = DataIngestion()
        self.text_analyzer = TextAnalyzer()
        
    def analyze_impact(self, cutoff_date):
        """Analyze the impact of development items"""
        try:
            # Get the data
            csat_df, tickets_df, dev_tickets_df = self.data_ingestion.get_combined_data()
            
            # Get sentiment and themes
            sentiment_results = self.text_analyzer.analyze_all(csat_df, tickets_df)
            
            # Calculate impact metrics
            impact_metrics = self._calculate_impact(
                tickets_df,
                dev_tickets_df,
                sentiment_results
            )
            
            return impact_metrics
            
        except Exception as e:
            logger.error(f"Error in impact analysis: {str(e)}")
            raise
    
    def _calculate_impact(self, tickets_df, dev_tickets_df, sentiment_results):
        """Calculate impact metrics for development items"""
        try:
            # Extract themes from sentiment results
            themes = sentiment_results.get('themes', [])
            
            impact_scores = []
            
            for _, dev_item in dev_tickets_df.iterrows():
                score = {
                    'ticket_id': dev_item['ticket_id'],
                    'title': dev_item['title'],
                    'priority': dev_item['priority'],
                    'story_points': dev_item['story_points']
                }
                
                # Calculate priority score
                priority_score = {
                    'High': 3,
                    'Medium': 2,
                    'Low': 1
                }.get(dev_item['priority'], 1)
                
                # Calculate composite score using themes and priority
                score['composite_score'] = (
                    (priority_score * 0.6) +
                    (dev_item['story_points'] / 34 * 0.4)  # Normalize story points
                )
                
                impact_scores.append(score)
            
            # Convert to DataFrame and sort by composite score
            impact_df = pd.DataFrame(impact_scores)
            impact_df = impact_df.sort_values('composite_score', ascending=False)
            
            return impact_df
            
        except Exception as e:
            logger.error(f"Error calculating impact: {str(e)}")
            raise

    def _calculate_impact_old(self, development_date: datetime) -> ImpactMetrics:
        """Calculate impact metrics before and after development change"""
        # Load and analyze data
        ingestion = DataIngestion()
        analyzer = TextAnalyzer()
        
        csat_df = ingestion.load_csat_data()
        tickets_df = ingestion.load_support_tickets()
        
        analysis_results = analyzer.analyze_all(csat_df, tickets_df)
        
        # Combine analyses for overall impact
        impact_metrics = self._calculate_impact(
            analysis_results.ticket_analysis,
            development_date
        )
        
        # Visualize results
        self._plot_impact(
            analysis_results.ticket_analysis,
            development_date
        )
        
        return impact_metrics
    
    def _calculate_impact(
        self,
        tickets_df: pd.DataFrame,
        dev_tickets_df: pd.DataFrame,
        sentiment_results: Dict[str, pd.DataFrame]
    ) -> ImpactMetrics:
        """Calculate impact metrics for development items"""
        try:
            # Extract themes from sentiment results
            themes = sentiment_results.get('themes', [])
            
            impact_scores = []
            
            for _, dev_item in dev_tickets_df.iterrows():
                score = {
                    'ticket_id': dev_item['ticket_id'],
                    'title': dev_item['title'],
                    'priority': dev_item['priority'],
                    'story_points': dev_item['story_points']
                }
                
                # Calculate priority score
                priority_score = {
                    'High': 3,
                    'Medium': 2,
                    'Low': 1
                }.get(dev_item['priority'], 1)
                
                # Calculate composite score using themes and priority
                score['composite_score'] = (
                    (priority_score * 0.6) +
                    (dev_item['story_points'] / 34 * 0.4)  # Normalize story points
                )
                
                impact_scores.append(score)
            
            # Convert to DataFrame and sort by composite score
            impact_df = pd.DataFrame(impact_scores)
            impact_df = impact_df.sort_values('composite_score', ascending=False)
            
            return impact_df
            
        except Exception as e:
            logger.error(f"Error calculating impact: {str(e)}")
            raise
    
    def _plot_impact(
        self,
        tickets_df: pd.DataFrame,
        development_date: datetime
    ):
        """Create visualization of impact analysis"""
        plt.figure(figsize=(12, 6))
        
        # Combine sentiment scores
        tickets_df['source'] = 'Tickets'
        ticket_data = tickets_df[['created_date', 'sentiment_score', 'source']]
        
        # Rename date columns for consistency
        ticket_data = ticket_data.rename(columns={'created_date': 'date'})
        
        # Plot sentiment over time
        sns.scatterplot(data=ticket_data, x='date', y='sentiment_score', hue='source', alpha=0.5)
        plt.axvline(x=development_date, color='r', linestyle='--', label='Development Change')
        
        plt.title('Sentiment Scores Before and After Development Change')
        plt.xlabel('Date')
        plt.ylabel('Sentiment Score')
        plt.legend()
        
        # Save plot
        plt.savefig('impact_analysis.png')
        plt.close()

if __name__ == "__main__":
    # Example usage
    analyzer = ImpactAnalyzer()
    impact = analyzer.analyze_impact(development_date=datetime(2024, 1, 1))
    
    print(f"Sentiment Change: {impact.sentiment_change:.3f}")
    print(f"Statistical Significance: p={impact.p_value:.3f}")
    print("\nTheme Changes:")
    for theme, change in impact.theme_changes.items():
        print(f"{theme}: {change:.3f}")
    print("\nSignificant Themes:")
    for theme in impact.significant_themes:
        print(f"- {theme}") 