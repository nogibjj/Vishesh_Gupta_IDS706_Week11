The operation is load data

The truncated output is: 

|    |   Round | Date            | Team_1               | FT   | Team_2                     |   id |   Team1_Score |   Team2_Score | Result   |
|---:|--------:|:----------------|:---------------------|:-----|:---------------------------|-----:|--------------:|--------------:|:---------|
|  0 |       1 | Fri Aug 9 2019  | Liverpool FC         | 4-1  | Norwich City FC            |    0 |             4 |             1 | Win      |
|  1 |       1 | Sat Aug 10 2019 | West Ham United FC   | 0-5  | Manchester City FC         |    1 |             0 |             5 | Loss     |
|  2 |       1 | Sat Aug 10 2019 | Burnley FC           | 3-0  | Southampton FC             |    2 |             3 |             0 | Win      |
|  3 |       1 | Sat Aug 10 2019 | AFC Bournemouth      | 1-1  | Sheffield United FC        |    3 |             1 |             1 | Draw     |
|  4 |       1 | Sat Aug 10 2019 | Crystal Palace FC    | 0-0  | Everton FC                 |    4 |             0 |             0 | Draw     |
|  5 |       1 | Sat Aug 10 2019 | Watford FC           | 0-3  | Brighton & Hove Albion FC  |    5 |             0 |             3 | Loss     |
|  6 |       1 | Sat Aug 10 2019 | Tottenham Hotspur FC | 3-1  | Aston Villa FC             |    6 |             3 |             1 | Win      |
|  7 |       1 | Sun Aug 11 2019 | Leicester City FC    | 0-0  | Wolverhampton Wanderers FC |    7 |             0 |             0 | Draw     |
|  8 |       1 | Sun Aug 11 2019 | Newcastle United FC  | 0-1  | Arsenal FC                 |    8 |             0 |             1 | Loss     |
|  9 |       1 | Sun Aug 11 2019 | Manchester United FC | 4-0  | Chelsea FC                 |    9 |             4 |             0 | Win      |

The operation is query data

The query is 
        SELECT Round, COUNT(*) AS match_count 
        FROM match_data_delta 
        GROUP BY Round 
        ORDER BY Round
        

The truncated output is: 

|    |   Round |   match_count |
|---:|--------:|--------------:|
|  0 |       1 |            10 |
|  1 |       2 |            10 |
|  2 |       3 |            10 |
|  3 |       4 |            10 |
|  4 |       5 |            10 |
|  5 |       6 |            10 |
|  6 |       7 |            10 |
|  7 |       8 |            10 |
|  8 |       9 |            10 |
|  9 |      10 |            10 |
| 10 |      11 |            10 |
| 11 |      12 |            10 |
| 12 |      13 |            10 |
| 13 |      14 |            10 |
| 14 |      15 |            10 |
| 15 |      16 |            10 |
| 16 |      17 |            10 |
| 17 |      18 |            10 |
| 18 |      19 |            10 |
| 19 |      20 |            10 |
| 20 |      21 |            10 |
| 21 |      22 |            10 |
| 22 |      23 |            10 |
| 23 |      24 |            10 |
| 24 |      25 |            10 |
| 25 |      26 |            10 |
| 26 |      27 |            10 |
| 27 |      28 |            10 |
| 28 |      29 |            10 |
| 29 |      30 |            10 |
| 30 |      31 |            10 |
| 31 |      32 |            10 |
| 32 |      33 |            10 |
| 33 |      34 |            10 |
| 34 |      35 |            10 |
| 35 |      36 |            10 |
| 36 |      37 |            10 |
| 37 |      38 |            10 |

The operation is load data

The truncated output is: 

|    |   Round | Date            | Team_1               | FT   | Team_2                     |   id |   Team1_Score |   Team2_Score | Result   |
|---:|--------:|:----------------|:---------------------|:-----|:---------------------------|-----:|--------------:|--------------:|:---------|
|  0 |       1 | Fri Aug 9 2019  | Liverpool FC         | 4-1  | Norwich City FC            |    0 |             4 |             1 | Win      |
|  1 |       1 | Sat Aug 10 2019 | West Ham United FC   | 0-5  | Manchester City FC         |    1 |             0 |             5 | Loss     |
|  2 |       1 | Sat Aug 10 2019 | Burnley FC           | 3-0  | Southampton FC             |    2 |             3 |             0 | Win      |
|  3 |       1 | Sat Aug 10 2019 | AFC Bournemouth      | 1-1  | Sheffield United FC        |    3 |             1 |             1 | Draw     |
|  4 |       1 | Sat Aug 10 2019 | Crystal Palace FC    | 0-0  | Everton FC                 |    4 |             0 |             0 | Draw     |
|  5 |       1 | Sat Aug 10 2019 | Watford FC           | 0-3  | Brighton & Hove Albion FC  |    5 |             0 |             3 | Loss     |
|  6 |       1 | Sat Aug 10 2019 | Tottenham Hotspur FC | 3-1  | Aston Villa FC             |    6 |             3 |             1 | Win      |
|  7 |       1 | Sun Aug 11 2019 | Leicester City FC    | 0-0  | Wolverhampton Wanderers FC |    7 |             0 |             0 | Draw     |
|  8 |       1 | Sun Aug 11 2019 | Newcastle United FC  | 0-1  | Arsenal FC                 |    8 |             0 |             1 | Loss     |
|  9 |       1 | Sun Aug 11 2019 | Manchester United FC | 4-0  | Chelsea FC                 |    9 |             4 |             0 | Win      |

The operation is query data

The query is 
        SELECT Round, COUNT(*) AS match_count 
        FROM match_data_delta 
        GROUP BY Round 
        ORDER BY Round
        

The truncated output is: 

|    |   Round |   match_count |
|---:|--------:|--------------:|
|  0 |       1 |            10 |
|  1 |       2 |            10 |
|  2 |       3 |            10 |
|  3 |       4 |            10 |
|  4 |       5 |            10 |
|  5 |       6 |            10 |
|  6 |       7 |            10 |
|  7 |       8 |            10 |
|  8 |       9 |            10 |
|  9 |      10 |            10 |
| 10 |      11 |            10 |
| 11 |      12 |            10 |
| 12 |      13 |            10 |
| 13 |      14 |            10 |
| 14 |      15 |            10 |
| 15 |      16 |            10 |
| 16 |      17 |            10 |
| 17 |      18 |            10 |
| 18 |      19 |            10 |
| 19 |      20 |            10 |
| 20 |      21 |            10 |
| 21 |      22 |            10 |
| 22 |      23 |            10 |
| 23 |      24 |            10 |
| 24 |      25 |            10 |
| 25 |      26 |            10 |
| 26 |      27 |            10 |
| 27 |      28 |            10 |
| 28 |      29 |            10 |
| 29 |      30 |            10 |
| 30 |      31 |            10 |
| 31 |      32 |            10 |
| 32 |      33 |            10 |
| 33 |      34 |            10 |
| 34 |      35 |            10 |
| 35 |      36 |            10 |
| 36 |      37 |            10 |
| 37 |      38 |            10 |

The operation is load data

The truncated output is: 

|    |   Round | Date            | Team_1               | FT   | Team_2                     |   id |   Team1_Score |   Team2_Score | Result   |
|---:|--------:|:----------------|:---------------------|:-----|:---------------------------|-----:|--------------:|--------------:|:---------|
|  0 |       1 | Fri Aug 9 2019  | Liverpool FC         | 4-1  | Norwich City FC            |    0 |             4 |             1 | Win      |
|  1 |       1 | Sat Aug 10 2019 | West Ham United FC   | 0-5  | Manchester City FC         |    1 |             0 |             5 | Loss     |
|  2 |       1 | Sat Aug 10 2019 | Burnley FC           | 3-0  | Southampton FC             |    2 |             3 |             0 | Win      |
|  3 |       1 | Sat Aug 10 2019 | AFC Bournemouth      | 1-1  | Sheffield United FC        |    3 |             1 |             1 | Draw     |
|  4 |       1 | Sat Aug 10 2019 | Crystal Palace FC    | 0-0  | Everton FC                 |    4 |             0 |             0 | Draw     |
|  5 |       1 | Sat Aug 10 2019 | Watford FC           | 0-3  | Brighton & Hove Albion FC  |    5 |             0 |             3 | Loss     |
|  6 |       1 | Sat Aug 10 2019 | Tottenham Hotspur FC | 3-1  | Aston Villa FC             |    6 |             3 |             1 | Win      |
|  7 |       1 | Sun Aug 11 2019 | Leicester City FC    | 0-0  | Wolverhampton Wanderers FC |    7 |             0 |             0 | Draw     |
|  8 |       1 | Sun Aug 11 2019 | Newcastle United FC  | 0-1  | Arsenal FC                 |    8 |             0 |             1 | Loss     |
|  9 |       1 | Sun Aug 11 2019 | Manchester United FC | 4-0  | Chelsea FC                 |    9 |             4 |             0 | Win      |

The operation is query data

The query is 
        SELECT Round, COUNT(*) AS match_count 
        FROM match_data_delta 
        GROUP BY Round 
        ORDER BY Round
        

The truncated output is: 

|    |   Round |   match_count |
|---:|--------:|--------------:|
|  0 |       1 |            10 |
|  1 |       2 |            10 |
|  2 |       3 |            10 |
|  3 |       4 |            10 |
|  4 |       5 |            10 |
|  5 |       6 |            10 |
|  6 |       7 |            10 |
|  7 |       8 |            10 |
|  8 |       9 |            10 |
|  9 |      10 |            10 |
| 10 |      11 |            10 |
| 11 |      12 |            10 |
| 12 |      13 |            10 |
| 13 |      14 |            10 |
| 14 |      15 |            10 |
| 15 |      16 |            10 |
| 16 |      17 |            10 |
| 17 |      18 |            10 |
| 18 |      19 |            10 |
| 19 |      20 |            10 |
| 20 |      21 |            10 |
| 21 |      22 |            10 |
| 22 |      23 |            10 |
| 23 |      24 |            10 |
| 24 |      25 |            10 |
| 25 |      26 |            10 |
| 26 |      27 |            10 |
| 27 |      28 |            10 |
| 28 |      29 |            10 |
| 29 |      30 |            10 |
| 30 |      31 |            10 |
| 31 |      32 |            10 |
| 32 |      33 |            10 |
| 33 |      34 |            10 |
| 34 |      35 |            10 |
| 35 |      36 |            10 |
| 36 |      37 |            10 |
| 37 |      38 |            10 |

The operation is load data

The truncated output is: 

|    |   Round | Date            | Team_1               | FT   | Team_2                     |   id |   Team1_Score |   Team2_Score | Result   |
|---:|--------:|:----------------|:---------------------|:-----|:---------------------------|-----:|--------------:|--------------:|:---------|
|  0 |       1 | Fri Aug 9 2019  | Liverpool FC         | 4-1  | Norwich City FC            |    0 |             4 |             1 | Win      |
|  1 |       1 | Sat Aug 10 2019 | West Ham United FC   | 0-5  | Manchester City FC         |    1 |             0 |             5 | Loss     |
|  2 |       1 | Sat Aug 10 2019 | Burnley FC           | 3-0  | Southampton FC             |    2 |             3 |             0 | Win      |
|  3 |       1 | Sat Aug 10 2019 | AFC Bournemouth      | 1-1  | Sheffield United FC        |    3 |             1 |             1 | Draw     |
|  4 |       1 | Sat Aug 10 2019 | Crystal Palace FC    | 0-0  | Everton FC                 |    4 |             0 |             0 | Draw     |
|  5 |       1 | Sat Aug 10 2019 | Watford FC           | 0-3  | Brighton & Hove Albion FC  |    5 |             0 |             3 | Loss     |
|  6 |       1 | Sat Aug 10 2019 | Tottenham Hotspur FC | 3-1  | Aston Villa FC             |    6 |             3 |             1 | Win      |
|  7 |       1 | Sun Aug 11 2019 | Leicester City FC    | 0-0  | Wolverhampton Wanderers FC |    7 |             0 |             0 | Draw     |
|  8 |       1 | Sun Aug 11 2019 | Newcastle United FC  | 0-1  | Arsenal FC                 |    8 |             0 |             1 | Loss     |
|  9 |       1 | Sun Aug 11 2019 | Manchester United FC | 4-0  | Chelsea FC                 |    9 |             4 |             0 | Win      |

The operation is query data

The query is 
        SELECT Round, COUNT(*) AS match_count 
        FROM match_data_delta 
        GROUP BY Round 
        ORDER BY Round
    

The truncated output is: 

|    |   Round |   match_count |
|---:|--------:|--------------:|
|  0 |       1 |            10 |
|  1 |       2 |            10 |
|  2 |       3 |            10 |
|  3 |       4 |            10 |
|  4 |       5 |            10 |
|  5 |       6 |            10 |
|  6 |       7 |            10 |
|  7 |       8 |            10 |
|  8 |       9 |            10 |
|  9 |      10 |            10 |

The operation is load data

The truncated output is: 

|    |   Round | Date            | Team_1               | FT   | Team_2                     |   id |   Team1_Score |   Team2_Score | Result   |
|---:|--------:|:----------------|:---------------------|:-----|:---------------------------|-----:|--------------:|--------------:|:---------|
|  0 |       1 | Fri Aug 9 2019  | Liverpool FC         | 4-1  | Norwich City FC            |    0 |             4 |             1 | Win      |
|  1 |       1 | Sat Aug 10 2019 | West Ham United FC   | 0-5  | Manchester City FC         |    1 |             0 |             5 | Loss     |
|  2 |       1 | Sat Aug 10 2019 | Burnley FC           | 3-0  | Southampton FC             |    2 |             3 |             0 | Win      |
|  3 |       1 | Sat Aug 10 2019 | AFC Bournemouth      | 1-1  | Sheffield United FC        |    3 |             1 |             1 | Draw     |
|  4 |       1 | Sat Aug 10 2019 | Crystal Palace FC    | 0-0  | Everton FC                 |    4 |             0 |             0 | Draw     |
|  5 |       1 | Sat Aug 10 2019 | Watford FC           | 0-3  | Brighton & Hove Albion FC  |    5 |             0 |             3 | Loss     |
|  6 |       1 | Sat Aug 10 2019 | Tottenham Hotspur FC | 3-1  | Aston Villa FC             |    6 |             3 |             1 | Win      |
|  7 |       1 | Sun Aug 11 2019 | Leicester City FC    | 0-0  | Wolverhampton Wanderers FC |    7 |             0 |             0 | Draw     |
|  8 |       1 | Sun Aug 11 2019 | Newcastle United FC  | 0-1  | Arsenal FC                 |    8 |             0 |             1 | Loss     |
|  9 |       1 | Sun Aug 11 2019 | Manchester United FC | 4-0  | Chelsea FC                 |    9 |             4 |             0 | Win      |

The operation is query data

The query is 
        SELECT Round, COUNT(*) AS match_count 
        FROM match_data_delta 
        GROUP BY Round 
        ORDER BY Round
    

The truncated output is: 

|    |   Round |   match_count |
|---:|--------:|--------------:|
|  0 |       1 |            10 |
|  1 |       2 |            10 |
|  2 |       3 |            10 |
|  3 |       4 |            10 |
|  4 |       5 |            10 |
|  5 |       6 |            10 |
|  6 |       7 |            10 |
|  7 |       8 |            10 |
|  8 |       9 |            10 |
|  9 |      10 |            10 |

