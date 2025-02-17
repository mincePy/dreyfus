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
    
    def _calculate_theme_frequency(self, tickets_df: pd.DataFrame, sentiment_results: Dict) -> Dict[str, float]:
        """Calculate normalized frequency and sentiment impact of each theme"""
        theme_stats = {}
        themes = sentiment_results.get('themes', {})
        
        for theme, theme_data in themes.items():
            # Calculate theme frequency
            frequency = len(theme_data['occurrences']) / len(tickets_df)
            
            # Calculate average sentiment for this theme
            theme_sentiment = np.mean(theme_data['sentiments'])
            
            # Themes with negative sentiment get higher weight
            sentiment_weight = 2.0 if theme_sentiment < 0 else 1.0
            
            theme_stats[theme] = frequency * sentiment_weight
            
        return theme_stats
    
    def _calculate_theme_score(self, dev_item: pd.Series, theme_stats: Dict[str, float]) -> float:
        """Calculate theme relevance score for a development item"""
        # Extract themes from development item title and description
        item_text = f"{dev_item['title']} {dev_item.get('description', '')}"
        item_themes = self.text_analyzer.extract_themes(item_text)
        
        # Sum the importance scores of matching themes
        theme_score = sum(theme_stats.get(theme, 0) for theme in item_themes)
        
        # Normalize to 0-1 range
        max_possible_score = max(theme_stats.values()) if theme_stats else 1
        normalized_score = theme_score / max_possible_score if max_possible_score > 0 else 0
        
        return normalized_score

    def _calculate_impact(self, tickets_df, dev_tickets_df, sentiment_results):
        """Calculate impact metrics for development items"""
        try:
            # Calculate theme statistics
            theme_stats = self._calculate_theme_frequency(tickets_df, sentiment_results)
            
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
                
                # Normalize story points (assuming 34 is max story points)
                story_points_score = dev_item['story_points'] / 34
                
                # Calculate theme relevance score
                theme_score = self._calculate_theme_score(dev_item, theme_stats)
                
                # Calculate composite score with three components
                score['composite_score'] = (
                    (priority_score / 3 * 0.4) +      # Priority (normalized to 0-1) is 40%
                    (story_points_score * 0.3) +      # Story points is 30%
                    (theme_score * 0.3)               # Theme relevance is 30%
                )
                
                # Add theme analysis details for transparency
                score['theme_score'] = theme_score
                score['relevant_themes'] = self.text_analyzer.extract_themes(dev_item['title'])
                
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
    impact_df = analyzer.analyze_impact(development_date=datetime(2024, 1, 1))
    
    # Generate HTML report
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .metric {{ margin-bottom: 20px; }}
            .theme {{ margin-left: 20px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .high-impact {{ background-color: #ffebee; }}
            .medium-impact {{ background-color: #fff3e0; }}
        </style>
    </head>
    <body>
        <h1>Development Impact Analysis Report</h1>
        
        <h2>Top Priority Items</h2>
        <table>
            <tr>
                <th>Ticket ID</th>
                <th>Title</th>
                <th>Priority</th>
                <th>Story Points</th>
                <th>Theme Score</th>
                <th>Composite Score</th>
                <th>Relevant Themes</th>
            </tr>
            {''.join(
                f"<tr class='{'high-impact' if row['composite_score'] > 0.7 else 'medium-impact' if row['composite_score'] > 0.4 else ''}'>"
                f"<td>{row['ticket_id']}</td>"
                f"<td>{row['title']}</td>"
                f"<td>{row['priority']}</td>"
                f"<td>{row['story_points']}</td>"
                f"<td>{row['theme_score']:.2f}</td>"
                f"<td>{row['composite_score']:.2f}</td>"
                f"<td>{', '.join(row['relevant_themes'])}</td>"
                "</tr>"
                for _, row in impact_df.head(10).iterrows()  # Show top 10 items
            )}
        </table>
        
        <h2>Analysis Summary</h2>
        <div class="metric">
            <p><strong>Total Items Analyzed:</strong> {len(impact_df)}</p>
            <p><strong>Average Impact Score:</strong> {impact_df['composite_score'].mean():.2f}</p>
            <p><strong>High Impact Items (>0.7):</strong> {len(impact_df[impact_df['composite_score'] > 0.7])}</p>
        </div>
    </body>
    </html>
    """
    
    # Save the HTML report
    with open('impact_analysis_report.html', 'w') as f:
        f.write(html_content)
    
    print("Report generated: impact_analysis_report.html") 