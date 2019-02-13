#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'out'))
	print(os.getcwd())
except:
	pass
#%% [markdown]
# ## 1) Degree Distribution
# 
# For the graphs provided to you, test and report which graphs are scalefree,
# namely whose degree distribution follows a power law , at least asymptotically. That is, the fraction P(k) of
# nodes in the network having k connections to other nodes goes for large values of k as
# 
# \begin{equation*}
# \ P(k) \sim k^{(-\gamma)} 
# \end{equation*}
# 
# where γ is a parameter whose value is typically in the range 2 < γ < 3, although occasionally it
# may lie outside these bounds.
# 
# Answer the following questions:
# 1. Generate a few random graphs. You can do this using networkx’s random graph generators or GTGraph . Do the random graphs you tested appear to be scalefree?
# 
# 2. Do the Stanford graphs provided to you appear to be scalefree?
# 

#%%
import glob
import pandas as pd
import numpy as np
from IPython.display import display, HTML

files = glob.glob('degree-outputs/*')

result = {
    'filename': [],
    'alpha': [],
    'scalefree': []
}

for f in files:
    name = f.split('/')[1]
    result['filename'].append(name)
    
    df = pd.read_csv(f)
    # Show DF for the random generated graphs     
    if len(name.split('.')) == 2:
        print(name)
        display(df)
    
    count = list(df['count'])
    degree = list(df['degree'])
    total_nodes = sum(count)
    fraction = [float(c)/total_nodes for c in count]
    slope, intercept = np.polyfit(np.log(degree), np.log(fraction), 1)
    
    result['alpha'].append(abs(slope))
    result['scalefree'].append('True' if abs(slope) > 1 and abs(slope) < 3.5 else 'False')
    
df = pd.DataFrame(data=result)
display(df)

#%% [markdown]
# ## 2 - Centrality
# 
# Answer the following questions about the graph:
# 
# 1. Rank the nodes from highest to lowest closeness centrality.

#%%
df = pd.read_csv('closeness.csv')
df

#%% [markdown]
# 2. Suppose we had some centralized data that would sit on one machine but would be
# shared with all computers on the network. Which two machines would be the best
# candidates to hold this data based on other machines having few hops to access this
# data?
#    * Ans : F and C as they have the most degree of closeness amongst all other computers
#%% [markdown]
# ## 3 - Articulation
# Answer the following questions:
# 1. In this example, which members should have been targeted to best disrupt communication in the organization?

#%%
df = pd.read_csv('graphframe_false.csv')
display(df)

#%% [markdown]
# The above members should be targetted to best disrupt the organization

