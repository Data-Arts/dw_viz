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

app_ui = ui.page_fluid(
    ui.panel_title("Bottom Line Dashboard"),
    
    ui.panel_main(
        ui.navset_tab(
        ui.nav("Key Findings", ui.output_plot("plot")),
        ui.nav("Size", ui.output_plot("size")),
        ui.nav("Sector", "Sector Graph")
     )
    ),
    

)

def server(input, output, session):
    @output
    @render.plot(alt="simple line graph")
    def plot():
        g = create_barplot(df,var,hue='Sector',style='darkgrid',savepath=None)
    @output
    @render.plot(alt="Size Graph")
    def size():
       

        

        # Create bar plot
        # g = sns.barplot(x='year', y='Unrestricted_operating_bottomline', hue='Size', data=grouped_data, ax=ax)
        g = create_barplot(df,var,hue='Sector',style='darkgrid',savepath=None)
        
        
        # g = sns.barplot(x='year', y='Unrestricted_operating_bottomline', hue='Size', data=df)

        # set legend fontsize larger
        # sns.set(font_scale=4)

        # Set x-axis label
        #plt.xlabel('Year',size=20)

        # Set y-axis label
       # plt.ylabel('Mean Unrestricted_operating_bottomline',size=20)

        # Set title
        #plt.title('Mean Unrestricted_operating_bottomline by Year and Size',size=20)

        #plt.savefig('Mean_Unrestricted_operating_bottomline_by_Year_and_Size.png');

    
   

app = App(app_ui, server)