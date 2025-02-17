import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from pathlib import Path
from src.data_ingestion import DataIngestion
from src.impact_analysis import ImpactAnalyzer
from datetime import datetime
import logging
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DevelopmentVisualizer:
    def __init__(self):
        # Create output directories if they don't exist
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)
        
        self.viz_dir = self.output_dir / 'visualizations'
        self.viz_dir.mkdir(exist_ok=True)
        
        # Define movement indicators
        self.MOVE_UP = "⬆️"
        self.MOVE_DOWN = "⬇️"
        self.NO_CHANGE = "➖"
        
        # Define priority colors
        self.priority_colors = {
            'high': '#ff4d4d',
            'medium': '#ffd633',
            'low': '#70db70'
        }
        
        logger.info(f"Visualizations will be saved to: {self.viz_dir}")
    
    def create_visualizations(self):
        """Generate all visualizations"""
        try:
            # Get data
            data_ingestion = DataIngestion()
            csat_df, tickets_df, dev_tickets_df = data_ingestion.get_combined_data()
            
            # Get impact analysis
            impact_analyzer = ImpactAnalyzer()
            impact_df = impact_analyzer.analyze_impact(None)  # cutoff_date not used
            
            # Create priority table
            self._create_priority_table(impact_df)
            
            # Create impact charts
            self._create_impact_charts(impact_df)
            
            # Create summary report
            self._create_summary_report(impact_df)
            
            logger.info(f"All visualizations saved to: {self.viz_dir}")
            
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            raise
    
    def _create_priority_table(self, impact_df):
        """Create interactive priority table"""
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=['Ticket ID', 'Title', 'Priority', 'Impact Score'],
                font=dict(size=12, color='white'),
                fill_color='darkblue',
                align='left'
            ),
            cells=dict(
                values=[
                    impact_df['ticket_id'],
                    impact_df['title'],
                    impact_df['priority'],
                    impact_df['composite_score'].round(3)
                ],
                align='left'
            )
        )])
        
        output_path = self.viz_dir / 'priority_table.html'
        fig.write_html(str(output_path))
        logger.info(f"Priority table saved to: {output_path}")
    
    def _create_impact_charts(self, impact_df):
        """Create impact visualization charts"""
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Impact Score Distribution', 'Priority Distribution'),
            specs=[[{"type": "xy"}],  # First subplot: histogram
                   [{"type": "domain"}]]  # Second subplot: pie chart
        )
        
        # Impact score histogram
        fig.add_trace(
            go.Histogram(x=impact_df['composite_score'], name='Impact Score'),
            row=1, col=1
        )
        
        # Priority pie chart
        priority_counts = impact_df['priority'].value_counts()
        fig.add_trace(
            go.Pie(labels=priority_counts.index, values=priority_counts.values, name='Priority'),
            row=2, col=1
        )
        
        output_path = self.viz_dir / 'impact_analysis.html'
        fig.write_html(str(output_path))
        logger.info(f"Impact charts saved to: {output_path}")
    
    def _create_summary_report(self, impact_df):
        """Create text-based summary report"""
        top_priority = impact_df.nlargest(5, 'composite_score')
        
        report_content = [
            "# Development Impact Analysis Summary",
            "",
            "## Top Priority Items",
            *[f"- {row['ticket_id']}: {row['title']} (Impact Score: {row['composite_score']:.2f})"
              for _, row in top_priority.iterrows()],
            "",
            "## Priority Distribution",
            *[f"- {priority}: {count} items"
              for priority, count in impact_df['priority'].value_counts().items()],
            ""
        ]
        
        output_path = self.viz_dir / 'summary_report.md'
        output_path.write_text('\n'.join(report_content))
        logger.info(f"Summary report saved to: {output_path}")

if __name__ == "__main__":
    visualizer = DevelopmentVisualizer()
    visualizer.create_visualizations() 