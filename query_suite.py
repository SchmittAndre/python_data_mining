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
    def get_tts_by_ttsid(self, ttsid):
        """
        Retrieves full row of database table by given 'ttsid' (named 'zugid'
        in database).
        """
        query = "SELECT * FROM zuege WHERE zuege.zugid = \"{}\""\
            .format(ttsid)
        query = self._append_limit_to_query(query)
        
        cursor = self.dbc.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    
    
    def get_ttsid_like(self, dailytripid="", yymmddhhmm="", stopindex=""):
        """
        Retrieves all time table stop ids (ttsid, named 'zugid' in database) 
        that match the specified SQL pattern.
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
    def get_ttsid_on_trip(self, dailytripid, yymmddhhmm):
        """
        Retrieves all time table stop id (ttsid) (named zugid in data base) 
        of the stops that are part of the trip. The trip is determined by 
        matching the ttsid with the given dailytripid an datetime pattern. 
        Stations are sortet in the order of the trip.
        """
        qs = self
        q_ttsid = qs.get_ttsid_like(
            dailytripid=dailytripid, yymmddhhmm=yymmddhhmm)
        q_ttsid_sorted = qs.sort_by_stopindex(q_ttsid)
        return q_ttsid_sorted
        
    
    def sort_by_stopindex(self, data, column=0):
        """ 
        Sorts given data by the stop index of the dailytripid in ascending 
        order. 
        'column' specifies the column index of the tuples inside the data tuple
        that hold the dailytripid.
        """
        def accessing(x):
            dailytripid = x[column]
            stopindex = re.search("-?[0-9]*-[0-9]*-([0-9]+)", dailytripid)
            stopindex = stopindex.group(1)
            return int(stopindex)
            
        return sorted(data, key=accessing)