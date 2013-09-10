class Bandwidth:

    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    hosts = {}
    storeId = {}

    def add(self, host, bytes, date):
        newDate = self.parseDate(date)

        if (self.hosts.has_key(host) == False):
            self.hosts[host] = {}

        if (self.hosts[host].has_key(newDate) == False):
            self.hosts[host][newDate] = 0;

        self.hosts[host][newDate] += int(bytes)

    def parseDate(self, date):
        date = date[1:-1]
        return ('-'.join([date[7:11], self.months[date[3:6]], date[0:2]]))

    def getStoreId(self, domain):
        if (self.storeId.has_key(domain) == False):
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM store WHERE domain = %s" % self.connection.literal(domain))
            row = cursor.fetchone()
            
            if row is None:
                raise Exception("Store %s does not exists." % domain)
            
            self.storeId[domain] = row[0]

        return self.storeId[domain]

    def persist(self, connection):
        self.connection = connection

        for (host, items) in sorted(self.hosts.items()):
            for (date, bytes) in sorted(items.items()):
                storeId = self.getStoreId(host)
                cursor = self.connection.cursor()

                cursor.execute("SELECT store_id, date, bytes FROM log where store_id = %d AND date = '%s'" % (storeId, date))
                row = cursor.fetchone()

                cursor = self.connection.cursor()

                if row is None:
                    print cursor.execute("INSERT INTO log (store_id, date, bytes) VALUES (%d, '%s', %d)" % (storeId, date, bytes))
                else:
                    print cursor.execute("UPDATE log SET bytes = bytes + %d WHERE store_id = %d AND date = '%s'" % (bytes, storeId, date))

        self.connection.commit()
        self.connection.close()