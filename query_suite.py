import re


class QuerySuite:
    NO_QUERY_LIMIT = "none"
    dbc = None
    limit = 50
    
    
    def use_dbc(self, dbc):
        """
        Sets the data base connection to be used for queries.
        """
        self.dbc = dbc
        return self
        
        
    def set_limit(self, limit):
        """
        Sets the limit for sql queries. set the limit to the NO_QUERY_LIMIT
        constant to not limit the queries.
        """
        self.limit = limit
        return self

    
    # class helper functions ###################################################
    
    def _append_limit_to_query(self, query):
        """
        Takes a query as a string and appends a limit clause. If the limit value
        equals the NO_QUERY_LIMIT constant then no limit clause is appended.
        """
        if self.limit != self.NO_QUERY_LIMIT:
            query = query + " LIMIT {}".format(self.limit)
        return query
    
    
    # query processing functions ###############################################
    def select(self, data, columns):
        """ 
        Filters and extracts a specified column out if the query result
        args holds the indeces of the columns to extract.
        'data' holds the query result to be filtered.
        """
        result = () #empty tuple
        for row in data:
            newrow = () #empty tuple
            for c in columns:
                newrow = newrow + (row[c],)
            result = result + (newrow,)
        return result      
    
        
    # basic queries ############################################################
    def get_zug_by_zugid(self, zugid):
        """
        Retrieves full row of data base table by given 'zugid'.
        """
        query = "SELECT * FROM zuege WHERE zuege.zugid = \"{}\""\
            .format(zugid)
        query = self._append_limit_to_query(query)
        
        cursor = self.dbc.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    
    
    def get_zugid_like(self, dailytripid="", yymmddhhmm="", stopindex=""):
        """
        Retrieves all zugids that match the specified SQL pattern.
        'dailytripid' specifies pattern for the dailytripid.
        'yymmddhhmm' specifies the pattern for the second part of the zugid.
        'stopindex' specifies the pattern for the third part of the zugid.
        """
        if dailytripid == "":
            dailytripid = "%"
        if yymmddhhmm == "":
            yymmddhhmm = "%"
        if stopindex == "":
            stopindex = "%"
        query = "SELECT zuege.zugid FROM zuege WHERE zuege.zugid like \"{}-{}-{}\""\
            .format(dailytripid, yymmddhhmm, stopindex)
        query = self._append_limit_to_query(query)
        
        cursor = self.dbc.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
       
       
    def get_stationname_by_evanr(self, evanr):
        """
        Retrieves the station name of a given 'evanr'.
        """
        query = "SELECT NAME FROM haltestellen WHERE haltestellen.EVA_NR = \"{}\""\
            .format(evanr)
        query = self._append_limit_to_query(query)
        
        cursor = self.dbc.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
        
        
    # adcanced queries #########################################################
    def get_stations_on_dialytrip(self, dailytripid):
        """
        Retrieves evanr of all stations on the given dailytripid. Stations are
        sortet in the order of the trip.
        """
        qs = self
        result = ()
        q_zugid = qs.get_zugid_like(dailytripid=dailytripid)
        q_zugid_sorted = qs.sort_by_stationindex(q_zugid)
        
        for zugid in q_zugid_sorted:
            q_zug = qs.get_zug_by_zugid(zugid[0])
            q_evanr= qs.select(q_zug, columns=[9]) #select evanr
            result = result + q_evanr
        
        return result
        
    
    def sort_by_stationindex(self, data, column=0):
        """ 
        Sorts given data by the stationindex of the dailytripid in ascending 
        order. 
        'column' specifies the index of the inner tuple that holds the 
        dailytripid.
        """
        def accessing(x):
            dailytripid = x[column]
            stationindex = re.search("-?[0-9]*-[0-9]*-([0-9]+)", dailytripid)
            stationindex = stationindex.group(1)
            return int(stationindex)
            
        return sorted(data, key=accessing)