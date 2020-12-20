SELECT count(User_IP) as queries_number FROM UserLogs WHERE datediff(dd, UserLogs.TimeStamp, (?)) = 0;

