from dataframe import main
from list import roleList

for i in range(len(roleList)):
    print(f'starting {roleList[i][1]}')
    main(roleList[i])