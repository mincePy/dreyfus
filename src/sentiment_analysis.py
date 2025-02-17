import pandas as pd
import numpy as np
from transformers import pipeline
import logging

logger = logging.getLogger(__name__)

class TextAnalyzer:
    def __init__(self):
        self.sentiment_analyzer = pipeline("sentiment-analysis")
        
    def analyze_all(self, csat_df, tickets_df):
        """Analyze sentiment and themes in all text data"""
        try:
            # Combine feedback from both sources
            csat_text = self._prepare_csat_text(csat_df)
            ticket_text = self._prepare_ticket_text(tickets_df)
            
            # Analyze sentiment
            csat_sentiment = self._analyze_sentiment(csat_text)
            ticket_sentiment = self._analyze_sentiment(ticket_text)
            
            # Extract themes
            themes = self._extract_themes(csat_text + ticket_text)
            
            return {
                'csat_sentiment': csat_sentiment,
                'ticket_sentiment': ticket_sentiment,
                'themes': themes
            }
            
        except Exception as e:
            logger.error(f"Error in text analysis: {str(e)}")
            raise
    
    def _prepare_csat_text(self, df):
        """Prepare CSAT text for analysis"""
        texts = []
        if not df.empty:
            # Combine relevant text columns
            text_columns = ['reason_for_rating', 'feature_feedback', 'improvement_suggestions']
            for _, row in df.iterrows():
                combined_text = ' '.join(str(row[col]) for col in text_columns if col in row)
                if combined_text.strip():
                    texts.append(combined_text)
        return texts
    
    def _prepare_ticket_text(self, df):
        """Prepare support ticket text for analysis"""
        texts = []
        if not df.empty:
            # Combine relevant text columns
            text_columns = ['subject', 'description']
            for _, row in df.iterrows():
                combined_text = ' '.join(str(row[col]) for col in text_columns if col in row)
                if combined_text.strip():
                    texts.append(combined_text)
        return texts
    
    def _analyze_sentiment(self, texts):
        """Analyze sentiment in a list of texts"""
        if not texts:
            return []
        
        try:
            results = []
            for text in texts:
                if isinstance(text, str) and text.strip():
                    sentiment = self.sentiment_analyzer(text[:512])[0]  # Truncate to max length
                    results.append(sentiment)
            return results
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {str(e)}")
            return []
    
    def _extract_themes(self, texts):
        """Extract common themes from texts"""
        # Define common themes to look for
        common_themes = {
            'email': ['email', 'gmail', 'outlook'],
            'mobile': ['mobile', 'app', 'phone'],
            'performance': ['speed', 'slow', 'fast', 'performance'],
            'ui': ['interface', 'ui', 'design', 'layout'],
            'integration': ['integration', 'sync', 'connect'],
            'support': ['support', 'help', 'assistance']
        }
        
        # Count theme occurrences
        theme_counts = {theme: 0 for theme in common_themes}
        
        for text in texts:
            if isinstance(text, str):
                text_lower = text.lower()
                for theme, keywords in common_themes.items():
                    if any(keyword in text_lower for keyword in keywords):
                        theme_counts[theme] += 1
        
        # Return themes that appear at least once
        return [theme for theme, count in theme_counts.items() if count > 0]
