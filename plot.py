import plotly.graph_objects as go 
import plotly.express as px
import numpy as np

def plot_ts(df, nome, units, chart='line', source='Source: BIS, HedgeAnalytics', month_colors='', theme='light', 
            margins=[50, 50, 70, 70]):  # [left, right, top, bottom]
    fig = go.Figure()

    if len(df.columns) == 1:
        colors = ["#f1c40f"]
    if len(df.columns) == 2:
        colors = ["#f1c40f", "#2ecc71"]
    elif len(df.columns) == 3:
        colors = ["#f1c40f", "#2ecc71","#9b59b6","#e74c3c", "#bababa"]
    elif len(df.columns) == 4:
        colors = ["#f1c40f", "#2ecc71","#9b59b6","#e74c3c", "#bababa", "#0f3cf1"]
    elif len(df.columns) == 5:
        colors = ["#f1c40f", "#2ecc71","#9b59b6","#e74c3c","#bababa","#0f3cf1","#cc2e89"]
    else:
        colors = ["#f1c40f","#2ecc71", "#9b59b6", "#e74c3c", "#bababa", "#0f3cf1","#cc2e89",'#b69b59',"#5974b6","#3cd7e7", "#7d2eff", '#adf10f','#abecc7']
    
    # colors=["#f1c40f", "#2ecc71", "#9b59b6", "#e74c3c", "#bababa","#0f3cf1","#cc2e89",'#b69b59',"#5974b6","#3cd7e7", "#7d2eff", '#adf10f','#abecc7']

    if chart == 'Bar'or chart=='Bar_PCT' or chart == "bar":
        for i in range(len(df.columns)):
            fig.add_trace(go.Bar(
                x=df.index, y=df.iloc[:, i],
                marker=dict(color=colors[i]), # Adjusted for Bar trace
                name=str(df.columns[i])
            ))
            # fig.update_layout(bargap=0.01)

    elif chart == "regression":
        # Only works if there are at least 2 columns
        if df.shape[1] >= 2:
            x_all = df.iloc[:, 0]
            y_all = df.iloc[:, 1]
            # Remove rows where either is NaN
            mask = (~x_all.isna()) & (~y_all.isna())
            x_all = x_all[mask]
            y_all = y_all[mask]
            # Scatter all points
            fig.add_trace(go.Scatter(
                x=x_all, y=y_all, mode='markers',
                name=f"{df.columns[0]} vs {df.columns[1]} (All)",
                marker=dict(color="#3b7484", size=6, opacity=0.5)
            ))
            # Regression line
            if len(x_all) > 1:
                coef = np.polyfit(x_all, y_all, 1)
                reg_line = coef[0] * x_all + coef[1]
                fig.add_trace(go.Scatter(
                    x=x_all, y=reg_line, mode='lines',
                    name='Regression', line=dict(color="#ec772a", width=2)
                ))
            # Highlight latest 12 points
            x_recent = x_all.iloc[-12:]
            y_recent = y_all.iloc[-12:]
            fig.add_trace(go.Scatter(
                x=x_recent, y=y_recent, mode='markers',
                name="Latest 12", marker=dict(color="red", size=10, symbol="diamond"),
                showlegend=True
            ))
            fig.update_xaxes(title_text=str(df.columns[0]))
            fig.update_yaxes(title_text=str(df.columns[1]))

    elif chart == "distribution":
        # Violin plot for each series, overlay latest value
        for i, col in enumerate(df.columns):
            y = df[col].dropna()
            fig.add_trace(go.Violin(
                y=y,
                name=str(col),
                box_visible=True,
                meanline_visible=True,
                line_color=colors[i % len(colors)],
                opacity=0.7
            ))
            # Overlay the latest value as a scatter point
            if not y.empty:
                latest_value = y.iloc[-1]
                fig.add_trace(go.Scatter(
                    x=[str(col)],
                    y=[latest_value],
                    mode='markers',
                    marker=dict(color='red', size=14, symbol='diamond'),
                    name=f"Latest",
                    showlegend=(i == 0)
                ))

    else:
        for i in range(len(df.columns)):
            fig.add_trace(go.Scatter(
                    x=df.index, y=df.iloc[:, i], line=dict(color=colors[i], width=3), name=str(df.columns[i])))

    # Set text color based on theme
    text_color = '#0D1018' if theme == 'light' else '#FFFFFF'
    line_color = "black" if theme == 'light' else "white"
    zero_line_color = '#ededed' if theme == 'light' else '#333333'
    template_color = 'plotly_white' if theme == 'light' else 'plotly_dark'
    paper_color = 'rgba(250,250,250)' if theme == 'light' else 'rgba(30, 49, 66,1)'
    
    fig.add_annotation(
    text = (f"{source}")
    , showarrow=False
    , x = 0
    , y = -0.22
    , xref='paper'
    , yref='paper'
    , xanchor='left'
    , yanchor='bottom'
    , xshift=-1
    , yshift=-5
    , font=dict(size=10, color=text_color)
    , font_family= "Verdana"
    , align="left"
    ,)

    fig.add_annotation(
    text='<b>'+ nome +'<b>',
    xref='paper',
    yref='paper',
    x=0,
    y=1.25,
    showarrow=False,
    font=dict(size=20, color=text_color),
    align='left',
    yanchor='top'
    )

    if chart =='percent_change' or  chart == 'pct' or chart=='Bar_PCT'or chart=='BAR_PCT_INFLATION' or 'pct' in chart.lower():
            fig.update_layout(yaxis= { 'tickformat': ',.2%'})



    fig.update_xaxes(showgrid=False,showline=True, linewidth=1.2, linecolor='black',zeroline=True,zerolinecolor='#ededed')
    fig.update_yaxes(showgrid=False,showline=True, zerolinecolor='#ededed',linewidth=1.2, linecolor='black',zeroline=True)
    fig.update_layout(
        title={'text': '<b>' + '' + '<b>', 'y': 0.95, 'x': 0.075, 'xanchor': 'left', 'yanchor': 'top'},
        paper_bgcolor=paper_color,
        plot_bgcolor='rgba(0,0,0,0)',
        title_font_size=20,
        font_color=text_color,
        yaxis_title=units,
        template=template_color,
        font_family="Verdana",
        margin=dict(
            l=margins[0],
            r=margins[1],
            t=margins[2],
            b=margins[3]
        ),
        images=[dict(
            xref="paper", yref="paper",
            x=0.9, y=-0.2,
            sizex=0.2, sizey=0.2,
            source='https://raw.githubusercontent.com/ThresholdMacro/ThresholdMacro/main/Images/Sphere_no_letters.png',
            # xref="paper", yref="paper",
            # x=0.9, y=-0.2,
            # sizex=0.22, sizey=0.22,
            opacity=1,
            xanchor="center",
            yanchor="middle",
            sizing="contain",
            visible=True,
            layer="below"
        )],
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.0,
            xanchor="left",
            x=0,
            font_family='Verdana',
            font=dict(color=text_color)
        ),
        autosize=True,
        height=500,
    )

    fig.update_layout(
        yaxis=dict(
            linecolor=line_color, 
            showgrid=False, 
            tickwidth=1, 
            tickcolor=line_color, 
            ticks="inside",
            tickfont=dict(color=text_color)
        ),
        xaxis=dict(
            linecolor=line_color, 
            showgrid=False, 
            tickwidth=1, 
            tickcolor=line_color, 
            ticks="inside",
            tickfont=dict(color=text_color)
        )
    )

    fig.update_xaxes(
        showgrid=False, 
        showline=True, 
        linewidth=1.2, 
        linecolor=line_color, 
        zeroline=True, 
        zerolinecolor=zero_line_color
    )
    fig.update_yaxes(
        showgrid=False, 
        showline=True, 
        zerolinecolor=zero_line_color, 
        linewidth=1.2, 
        linecolor=line_color, 
        zeroline=True
    )

    return fig
