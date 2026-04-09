import streamlit as st
import pandas as pd
import time
import os
import plotly.express as px

st.set_page_config(
    page_title="Real-Time Fraud Detection Dashboard",
    page_icon="🕵️",
    layout="wide"
)

# Customize design with CSS
st.markdown("""
    <style>
    /* Sleek dark look */
    .stApp {
        background-color: #0E1117;
    }
    .metric-card {
        background-color: #1E293B;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        margin-bottom: 20px;
        text-align: center;
        border-left: 4px solid #F87171;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: bold;
        color: #F87171;
    }
    .metric-label {
        font-size: 1.1rem;
        color: #94A3B8;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🕵️ Real-Time Fraud Detection Pipeline")
st.markdown("Monitoring transactions streamed via **Kafka**, processed by **Apache Spark**, and analyzed by an **Isolation Forest** model.")
st.markdown("---")

ANOMALIES_FILE = "/app/data/anomalies.csv"

def read_data():
    if os.path.exists(ANOMALIES_FILE):
        try:
            return pd.read_csv(ANOMALIES_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

# Placeholder for real-time updates
placeholder = st.empty()

while True:
    df = read_data()
    
    with placeholder.container():
        if df.empty:
            st.info("Waiting for anomalies to be detected from the Kafka stream...")
        else:
            # Top Metrics
            col1, col2, col3 = st.columns(3)
            
            total_anomalies = len(df)
            total_value = df['amount'].sum() if 'amount' in df.columns else 0
            latest_type = df.iloc[-1]['type'] if 'type' in df.columns else 'N/A'
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Total Anomalies Detected</div>
                    <div class="metric-value">{total_anomalies:,}</div>
                </div>
                """, unsafe_allow_html=True)
                
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Value at Risk</div>
                    <div class="metric-value">${total_value:,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
                
            with col3:
                st.markdown(f"""
                <div class="metric-card" style="border-left: 4px solid #60A5FA;">
                    <div class="metric-label">Latest Type</div>
                    <div class="metric-value" style="color: #60A5FA;">{latest_type}</div>
                </div>
                """, unsafe_allow_html=True)
                
            
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                st.subheader("Anomalies by Transaction Type")
                if 'type' in df.columns:
                    type_counts = df['type'].value_counts().reset_index()
                    type_counts.columns = ['Type', 'Count']
                    fig_pie = px.pie(type_counts, values='Count', names='Type', 
                                     hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
                    fig_pie.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font_color="white",
                        margin=dict(t=30, b=0, l=0, r=0)
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                    
            with chart_col2:
                st.subheader("Anomaly Amount Distribution")
                if 'amount' in df.columns:
                    fig_hist = px.histogram(df, x="amount", nbins=40, 
                                            color_discrete_sequence=['#F87171'])
                    fig_hist.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font_color="white",
                        xaxis_title="Transaction Amount",
                        yaxis_title="Count",
                        margin=dict(t=30, b=0, l=0, r=0)
                    )
                    st.plotly_chart(fig_hist, use_container_width=True)
                    
            st.subheader("Recent Anomaly Logs")
            
            # Format dataframe slightly
            display_df = df.copy()
            if 'amount' in display_df.columns:
                display_df['amount'] = display_df['amount'].apply(lambda x: f"${x:,.2f}")
            
            st.dataframe(
                display_df.tail(20).iloc[::-1], 
                use_container_width=True,
                height=300
            )
            
    time.sleep(2)
