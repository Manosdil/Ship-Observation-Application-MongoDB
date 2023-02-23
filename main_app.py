from relational_queries import relational_query, sourcemmsi
from spatial_queries import knn, range_circle
from spatiotemporal_queries import spatiotemp_knn, spatiotemporal_range
from similar_queries import k_similar_traj

#libary for map visual
import plotly.express as px
token = 'pk.eyJ1IjoibWFub3NkaWxiIiwiYSI6ImNremJveG5rZzAydW4ycHAyenVmemg4MDUifQ.14DW6zRrWMYy5sTpyd3T3Q'
px.set_mapbox_access_token(token)

kind_of_query = int(input('What type of query do you want:\n1) Relational\n2) Spatial\n3) Spatio-Temporal\n4) Trajectories\nType the the corresponding number:'))

if kind_of_query == 1 :
    kind_of_relational = input('Do you want to check a specific sourcemmsi or other relational features\n1) Type sourcemmsi\n2) Type other\n')
    if kind_of_relational == 'sourcemmsi':
        data = sourcemmsi()
    elif kind_of_relational == 'other':
        data = relational_query()


elif kind_of_query == 2 :
    kind_of_spatial = input('What type of spatial query do you want:\n1) Range (type range)\n2) Knn (type knn)\n')
    if kind_of_spatial == 'range':
        data = range_circle()
    elif kind_of_spatial == 'knn':
        data = knn()

elif kind_of_query == 3 :
    kind_of_tempspat = input('What type of spatiotemporal query do you want:\n1) Range (type range)\n2) Knn (type knn)\n')
    if kind_of_tempspat == 'range':
        data = spatiotemporal_range()
    elif kind_of_tempspat == 'knn':
        data = spatiotemp_knn()
    
elif kind_of_query == 4 :
    data = k_similar_traj()

fig = px.scatter_mapbox(data, lat='lat', lon='lon', color = 'mmsi', hover_data=['mmsi', 'lat', 'lon'],zoom=5, color_continuous_scale=px.colors.cyclical.IceFire)
fig.show()
