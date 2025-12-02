"""
Anomaly Analytics and Root Cause Analysis
==========================================
Provides advanced analytics, root cause analysis, and reporting
for detected anomalies.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

import sys
sys.path.append('..')
from config.anomaly_config import ANALYTICS_CONFIG


class AnomalyAnalytics:
    """
    Advanced analytics for anomaly detection results.
    Provides root cause analysis, impact assessment, and reporting.
    """
    
    def __init__(self, config=None):
        """
        Initialize analytics module.
        
        Args:
            config (dict, optional): Analytics configuration. Defaults to ANALYTICS_CONFIG.
        """
        self.config = config or ANALYTICS_CONFIG
        self.rca_features = self.config.get("rca_features", [])
    
    def perform_root_cause_analysis(self, anomaly_row: pd.Series, 
                                    historical_data: pd.DataFrame) -> Dict:
        """
        Perform root cause analysis for a single anomaly.
        
        Args:
            anomaly_row: Row containing the anomaly
            historical_data: Historical data for comparison
            
        Returns:
            Dictionary with root cause insights
        """
        brand = anomaly_row.get('brand')
        timestamp = anomaly_row.get('window_start')
        
        # Get historical baseline for this brand
        brand_history = historical_data[historical_data['brand'] == brand]
        
        if len(brand_history) == 0:
            return {'error': 'No historical data available'}
        
        # Calculate deviations for key metrics
        deviations = {}
        
        # Views change
        if 'views' in anomaly_row and 'views' in brand_history.columns:
            baseline_views = brand_history['views'].median()
            current_views = anomaly_row['views']
            deviations['views_change'] = ((current_views - baseline_views) / baseline_views * 100) if baseline_views > 0 else 0
        
        # Conversion rate change
        if 'conversion_rate' in anomaly_row and 'conversion_rate' in brand_history.columns:
            baseline_conversion = brand_history['conversion_rate'].median()
            current_conversion = anomaly_row['conversion_rate']
            deviations['conversion_rate_change'] = ((current_conversion - baseline_conversion) / baseline_conversion * 100) if baseline_conversion > 0 else 0
        
        # Cart additions change
        if 'cart_additions' in anomaly_row and 'cart_additions' in brand_history.columns:
            baseline_cart = brand_history['cart_additions'].median()
            current_cart = anomaly_row.get('cart_additions', 0)
            deviations['cart_additions_change'] = ((current_cart - baseline_cart) / baseline_cart * 100) if baseline_cart > 0 else 0
        
        # Revenue change
        if 'revenue' in anomaly_row and 'revenue' in brand_history.columns:
            baseline_revenue = brand_history['revenue'].median()
            current_revenue = anomaly_row.get('revenue', 0)
            deviations['revenue_change'] = ((current_revenue - baseline_revenue) / baseline_revenue * 100) if baseline_revenue > 0 else 0
        
        # Average order value change
        if 'avg_order_value' in anomaly_row and 'avg_order_value' in brand_history.columns:
            baseline_aov = brand_history['avg_order_value'].median()
            current_aov = anomaly_row.get('avg_order_value', 0)
            deviations['avg_order_value_change'] = ((current_aov - baseline_aov) / baseline_aov * 100) if baseline_aov > 0 else 0
        
        # Identify primary contributing factors (top 3 deviations)
        sorted_deviations = sorted(deviations.items(), key=lambda x: abs(x[1]), reverse=True)
        primary_factors = [
            {'metric': metric, 'change_percent': change}
            for metric, change in sorted_deviations[:3]
        ]
        
        # Time-based factors
        time_factors = []
        if 'hour_of_day' in anomaly_row:
            hour = anomaly_row['hour_of_day']
            if hour < 6 or hour > 22:
                time_factors.append('Off-peak hours (low traffic)')
            elif 9 <= hour <= 17:
                time_factors.append('Business hours')
        
        if 'is_weekend' in anomaly_row and anomaly_row['is_weekend']:
            time_factors.append('Weekend')
        
        # Compile root cause report
        rca = {
            'brand': brand,
            'timestamp': timestamp,
            'anomaly_type': anomaly_row.get('anomaly_type', 'unknown'),
            'severity': anomaly_row.get('anomaly_severity', 'unknown'),
            'primary_factors': primary_factors,
            'time_factors': time_factors,
            'all_deviations': deviations,
            'summary': self._generate_rca_summary(primary_factors, time_factors, anomaly_row)
        }
        
        return rca
    
    def _generate_rca_summary(self, primary_factors: List[Dict], 
                             time_factors: List[str], 
                             anomaly_row: pd.Series) -> str:
        """Generate human-readable RCA summary."""
        anomaly_type = anomaly_row.get('anomaly_type', 'anomaly')
        severity = anomaly_row.get('anomaly_severity', 'unknown')
        
        summary_parts = [f"{severity.upper()} {anomaly_type} detected."]
        
        if primary_factors:
            top_factor = primary_factors[0]
            metric = top_factor['metric'].replace('_', ' ').title()
            change = top_factor['change_percent']
            direction = "increased" if change > 0 else "decreased"
            summary_parts.append(f"{metric} {direction} by {abs(change):.1f}%.")
        
        if time_factors:
            summary_parts.append(f"Occurred during: {', '.join(time_factors)}.")
        
        return " ".join(summary_parts)
    
    def assess_impact(self, anomalies: pd.DataFrame) -> Dict:
        """
        Assess overall business impact of anomalies.
        
        Args:
            anomalies: DataFrame with detected anomalies
            
        Returns:
            Dictionary with impact assessment
        """
        if len(anomalies) == 0:
            return {'total_anomalies': 0, 'total_impact': 0}
        
        impact = {
            'total_anomalies': len(anomalies),
            'anomalies_by_severity': anomalies['anomaly_severity'].value_counts().to_dict(),
            'anomalies_by_type': anomalies['anomaly_type'].value_counts().to_dict(),
        }
        
        # Revenue impact
        if 'estimated_revenue_impact' in anomalies.columns:
            total_revenue_impact = anomalies['estimated_revenue_impact'].sum()
            impact['total_revenue_impact'] = total_revenue_impact
            impact['avg_revenue_impact_per_anomaly'] = total_revenue_impact / len(anomalies)
            
            # Revenue impact by severity
            impact['revenue_impact_by_severity'] = anomalies.groupby('anomaly_severity')['estimated_revenue_impact'].sum().to_dict()
        
        # Conversion loss
        if 'estimated_conversion_loss' in anomalies.columns:
            total_conversion_loss = anomalies['estimated_conversion_loss'].sum()
            impact['total_conversion_loss'] = total_conversion_loss
            impact['avg_conversion_loss_per_anomaly'] = total_conversion_loss / len(anomalies)
        
        # Top affected brands
        if 'brand' in anomalies.columns:
            brand_counts = anomalies['brand'].value_counts().head(10)
            impact['top_affected_brands'] = brand_counts.to_dict()
        
        # Time distribution
        if 'hour_of_day' in anomalies.columns:
            hour_counts = anomalies['hour_of_day'].value_counts().sort_index()
            impact['anomalies_by_hour'] = hour_counts.to_dict()
        
        return impact
    
    def generate_alert_report(self, anomaly_row: pd.Series, 
                             historical_data: Optional[pd.DataFrame] = None) -> Dict:
        """
        Generate comprehensive alert report for a single anomaly.
        
        Args:
            anomaly_row: Row containing the anomaly
            historical_data: Historical data for RCA
            
        Returns:
            Dictionary with complete alert information
        """
        alert = {
            'alert_id': f"{anomaly_row.get('brand', 'unknown')}_{anomaly_row.get('window_start', '')}",
            'timestamp': anomaly_row.get('window_start'),
            'brand': anomaly_row.get('brand'),
            
            # Anomaly details
            'anomaly_type': anomaly_row.get('anomaly_type', 'unknown'),
            'severity': anomaly_row.get('anomaly_severity', 'unknown'),
            'confidence': anomaly_row.get('anomaly_confidence', 0),
            'ensemble_score': anomaly_row.get('ensemble_anomaly_score', 0),
            
            # Metrics
            'current_conversion_rate': anomaly_row.get('conversion_rate', 0),
            'expected_conversion_rate': anomaly_row.get('expected_value', 0),
            'deviation_percentage': anomaly_row.get('deviation_percentage', 0),
            
            'views': anomaly_row.get('views', 0),
            'purchases': anomaly_row.get('purchases', 0),
            'revenue': anomaly_row.get('revenue', 0),
            
            # Impact
            'estimated_revenue_impact': anomaly_row.get('estimated_revenue_impact', 0),
            'estimated_conversion_loss': anomaly_row.get('estimated_conversion_loss', 0),
            
            # Model scores
            'model_scores': {
                'lstm': anomaly_row.get('lstm_anomaly_score', 0),
                'prophet': anomaly_row.get('prophet_anomaly_score', 0),
                'isolation_forest': anomaly_row.get('isolation_forest_anomaly_score', 0),
                'changepoint': anomaly_row.get('changepoint_anomaly_score', 0),
            },
        }
        
        # Add RCA if historical data provided
        if historical_data is not None:
            alert['root_cause_analysis'] = self.perform_root_cause_analysis(anomaly_row, historical_data)
        
        # Priority based on severity and impact
        alert['priority'] = self._calculate_priority(alert)
        
        return alert
    
    def _calculate_priority(self, alert: Dict) -> str:
        """Calculate alert priority based on severity and impact."""
        severity = alert.get('severity', 'low')
        revenue_impact = abs(alert.get('estimated_revenue_impact', 0))
        
        # High priority: critical severity OR high revenue impact
        if severity == 'critical' or revenue_impact > 10000:
            return 'high'
        elif severity == 'high' or revenue_impact > 1000:
            return 'medium'
        else:
            return 'low'
    
    def create_dashboard_summary(self, anomalies: pd.DataFrame, 
                                time_range: str = '24h') -> Dict:
        """
        Create summary for dashboard display.
        
        Args:
            anomalies: DataFrame with anomalies
            time_range: Time range for summary
            
        Returns:
            Dashboard summary dictionary
        """
        summary = {
            'time_range': time_range,
            'last_updated': pd.Timestamp.now(),
            
            # Key metrics
            'total_anomalies': len(anomalies),
            'critical_anomalies': len(anomalies[anomalies['anomaly_severity'] == 'critical']),
            'high_anomalies': len(anomalies[anomalies['anomaly_severity'] == 'high']),
            
            # Trends
            'anomaly_trend': 'increasing',  # Would be calculated from historical comparison
            
            # Top issues
            'top_affected_brands': [],
            'highest_impact_anomaly': None,
        }
        
        if len(anomalies) > 0:
            # Top affected brands
            brand_counts = anomalies.groupby('brand').size().sort_values(ascending=False).head(5)
            summary['top_affected_brands'] = [
                {'brand': brand, 'count': int(count)}
                for brand, count in brand_counts.items()
            ]
            
            # Highest impact anomaly
            if 'estimated_revenue_impact' in anomalies.columns:
                worst_anomaly = anomalies.loc[anomalies['estimated_revenue_impact'].abs().idxmax()]
                summary['highest_impact_anomaly'] = {
                    'brand': worst_anomaly.get('brand'),
                    'timestamp': worst_anomaly.get('window_start'),
                    'impact': worst_anomaly.get('estimated_revenue_impact'),
                    'severity': worst_anomaly.get('anomaly_severity'),
                }
        
        return summary
    
    def export_report(self, anomalies: pd.DataFrame, 
                     output_format: str = 'dataframe') -> pd.DataFrame:
        """
        Export anomalies in report format.
        
        Args:
            anomalies: DataFrame with anomalies
            output_format: Output format ('dataframe', 'json', 'summary')
            
        Returns:
            Formatted report
        """
        if output_format == 'dataframe':
            # Select key columns for report
            report_columns = [
                'window_start', 'brand', 'anomaly_type', 'anomaly_severity',
                'conversion_rate', 'expected_value', 'deviation_percentage',
                'views', 'purchases', 'revenue',
                'estimated_revenue_impact', 'estimated_conversion_loss',
                'ensemble_anomaly_score', 'anomaly_confidence'
            ]
            
            available_columns = [col for col in report_columns if col in anomalies.columns]
            return anomalies[available_columns].copy()
        
        elif output_format == 'summary':
            # Create summary report
            summary = self.assess_impact(anomalies)
            return pd.DataFrame([summary])
        
        else:
            return anomalies


def create_analytics(config=None) -> AnomalyAnalytics:
    """
    Factory function to create analytics module.
    
    Args:
        config (dict, optional): Analytics configuration
        
    Returns:
        AnomalyAnalytics instance
    """
    return AnomalyAnalytics(config=config)
