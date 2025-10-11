from geopy.distance import great_circle

def calculate_distance(point_1: tuple, point_2: tuple, unit='km') -> float:
	""" Take 2 sets of coordinates and calculate the distance. """
	dist = great_circle(point_1, point_2)
	if unit.lower() == 'nm':
		return dist.nm
	else:
		return dist.km

def within_poi(ac_data: dict, poi: dict) -> bool:
	""" Return aircraft if it's inside the poi. """
	if 'lat' not in ac_data or 'lon' not in ac_data:
		return False
	ac_pos = (ac_data['lat'], ac_data['lon'])
	poi_pos = (poi['lat'], poi['lon'])
	dis = calculate_distance(ac_pos, poi_pos, unit=poi['unit'])
	return dis <= float(poi['distance'])
