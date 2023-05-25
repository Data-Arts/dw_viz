from collections import Counter
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
from utils import *
from scipy.stats import mstats

from shiny import App, render, ui

#import file
# filename = 'Unrestricted_operating_bottomline.csv'
# df = pd.read_csv(Path(__file__).parent / "Unrestricted_operating_bottomline.csv").sort_values(['organizations_id','year','Size','Sector']).reset_index(drop=True)

size_choices = ["Small","Medium","Large"]

df = create_cdpdataset()

#data-prep
df = winsorize_data(df,limits_winsorized,var)

 # Group data by year and type and calculate mean values
grouped_data = df.groupby(['year', 'Size']).mean(numeric_only=True).reset_index()

# Set seaborn style
sns.set_style('darkgrid')

#css / fonts
ui.tags.head(
  ui.tags.style(
    ui.HTML(
      "@import url('https://fonts.googleapis.com/css2?family=Open+Sans:ital,wght@0,300;0,400;0,800;1,500&display=swap'); body { color: #505050; font-family: 'Open Sans', sans-serif; padding-left: 20px; padding-right: 20px; } h1,h2 { font-family: 'Open Sans', sans-serif; color: #black; }"
  )
  )
)

style=""

app_ui = ui.page_fluid(
    ui.panel_title("Bottom Line Dashboard"),
    
    ui.panel_main(
        ui.navset_tab(
        ui.nav("Key Findings", 
               ui.row(
                   ui.column(6, ui.div(ui.h2("Theatre and Community Organizations Averaged a Positive Bottom Line in 2019"),ui.span("Community and Theater organizations operate on surpluses when analyzed along with all three measurements, with the Community sector coming out on top after accounting for depreciation. Performing Arts Centers (PACs) had the highest unrestricted surplus, and Other Museums had the highest operating surplus before depreciation.")), style=style),
                   ui.column(6, ui.output_plot("kf_1"), style=style),
               )),
        ui.nav("Size", 
               ui.row(
                  ui.div(
                      ui.h2("Small Orgizations Operating Bottom Line Stay Positive Over the Last Five Years"),
                      ui.p("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")
                  )
                ),
                ui.row(
                  ui.output_plot("size")
                ),
                ui.row(
                  ui.column(6,ui.input_select("size_picker","Size", size_choices)),
                  ui.column(6,ui.output_plot("size_Int"))
                )),
        ui.nav("Sector", ui.output_plot("plot"))
     )
    ),
    

)

def server(input, output, session):
    @output
    @render.plot()
    def size_Int():
        int =  create_OneBarplot(df=df,var='Unrestricted_operating_bottomline', yr=2022,sz=input.size_picker(),st='Theater', agg='mean', style='darkgrid',width=0.1,color='orange',alpha=1, xlabel_size=15,title_size=10,xticks_size=20,chartTitle="Theatre Unrestricted Operating Bottom Line")
    @output
    @render.plot(alt="Community Mean Operationing")
    def kf_1():
        kf = create_OneBarplot(df=df,var='Unrestricted_operating_bottomline', yr=[2020,2021,2022],sz='Small',st='Theater', agg='mean', style='darkgrid',width=0.1,color='orange',alpha=1, xlabel_size=15,title_size=10,xticks_size=20,chartTitle="Theatre Unrestricted Operating Bottom Line")
    @output
    @render.plot(alt="simple line graph")
    def plot():
        g = create_barplot(df,var,hue='Sector',style='darkgrid',savepath=None)
    @output
    @render.plot(alt="Size Graph")
    def size():
       

        

        # Create bar plot
        # g = sns.barplot(x='year', y='Unrestricted_operating_bottomline', hue='Size', data=grouped_data, ax=ax)
        g = create_barplot(df,var,hue='Size',style='darkgrid',savepath=None)
        


    
   

app = App(app_ui, server)