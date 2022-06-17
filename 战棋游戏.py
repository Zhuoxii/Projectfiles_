import sys
import copy


# Please implement this function according to Section "Read Configuration File"
def load_config_file(filepath):
    # It should return width, height, waters, woods, foods, golds based on the file
    # Complete the test driver of this function in file_loading_test.py

  f = open(filepath, 'r')
  if not filepath:
    raise FileNotFoundError
  title = ['Frame: ', 'Water: ', 'Wood: ', 'Food: ', 'Gold: ']
  ls = f.read().splitlines()
  f.close()
  for i in range(len(ls)):
    if i == 0 or i == 1:
      if ls[i][0:7] != title[i]:
        raise SyntaxError('Invalid Configuration File: format error!')
    else:
      if ls[i][0:6] != title[i]:
        raise SyntaxError('Invalid Configuration File: format error!')


  dict = {}
  for i in range(len(ls)):
      if i == 0:
        dict[title[i][0:-2]] = str(ls[i][len(title[i]):])
      else:
        dict[title[i][0:-2]] = ls[i][len(title[i]):].split()
         

  #dict = {'Frame': '5x5', 'Water': ['0', '0', '4', '2', '1', '3'], 'Wood': ['0', '2', '2', '4'], 'Food': ['0','4', '3', '1'], 'Gold': ['4', '1', '2','2'}
  
  number = []
  for key in dict:
      ls3 = []
      if key == 'Frame': 
          if (not dict[key][0].isdigit() or
              not dict[key][2].isdigit() or
              dict[key][1] != 'x'):
              raise SyntaxError('Invalid Configuration File: frame should be in format widthxheight!')
          if not 5 <= int(dict[key][0]) <= 7 or not 5 <= int(dict[key][2]) <= 7:
              raise ArithmeticError('Invalid Configuration File, width and height should range from 5 to 7!')
          number.append((int(dict[key][0]), int(dict[key][2])))
      
      else:
          ls2 = dict[key]
          j = 0
          while j < len(ls2):
            if not ls2[j].isdigit(): # non-integer
              raise ValueError(f'Invalid Configuration File: {key} contains non integer characters!')
            if len(ls2) % 2 != 0: #odd number     
              raise SyntaxError(f'Invalid Configuration File: {key} has an odd number of elements!')
            j += 1
          
          n = 0
          while n < len(ls2):
            ls3.append((ls2[n], ls2[n + 1]))
            n += 2
          number.append(ls3)
#number = [(5,5),[('0','0'),('4','2'),('3','3')],[('0','2'),('2','4')],[('0','4'),('3','1')],[('4','1'),('2','2')]]         
        # convert string into integer
  position = []
  lst = []
  for i in number:
    if number.index(i) == 0:
      position.append(i)
    if number.index(i) != 0:
      for m in i:
        lst.append(tuple(map(int,m)))
      lst = []
    position.append(lst)
  position = position[0:-1]
#position =[(5, 5), [(0, 0), (4, 2), (3, 3)], [(0, 2), (2, 4)], [(0, 4), (3, 1)], [(4, 1), (2, 2)]]
  
  m = 0
  while m < len(position):
    if m != 0:
      for i in position[m]:
        if i[0] > position[0][0] or i[1] > position[0][1]:
          raise ArithmeticError(f'Invalid Configuration File: {title[m][0:-2]} contains a position that is out of map.')
    m += 1
    
    

  occupied = [(1,1), (0,1), (1,0), (2,1), (1,2), 
              (position[0][0]-2, position[0][1]-2),  
              (position[0][0]-3, position[0][1]-2),
              (position[0][0]-2, position[0][1]-3),
              (position[0][0]-1, position[0][1]-2),
              (position[0][0]-2, position[0][1]-1)]
  for n in position[1:]:
    for i in n:
      if i in occupied:
        raise ValueError('Invalid Configuration File: The positions of home bases or the positions next to the home bases are occupied!')
  for e in position[1:]:
    if len(set(e)) != len(e):
      raise SyntaxError('Invalid Configuration File: Duplicate position (x, y)!') 
  print(f'Configuration file {filepath} was loaded.')
  width, height = position[0][0], position[0][1]
  waters, woods, foods, golds = position[1], position[2], position[3], position[4]  # list of position tuples
  return width, height, waters, woods, foods, golds

#Map Class
class Map:
  def __init__(self):
    self.width = width
    self.height = height
    self.board = [ ['  '] * width for i  in range(height)]
    for w in range(len(waters)):
        self.board[waters[w][0]][waters[w][1]] = '~~'
    for j in range(len(woods)):
        self.board[woods[j][0]][woods[j][1]] = 'WW'
    for f in range(len(foods)):
        self.board[foods[f][0]][foods[f][1]] = 'FF'
    for g in range(len(golds)):
        self.board[golds[g][0]][golds[g][1]] = 'GG'
    #Home base's coordinate
    self.board[1][1] = 'H1'
    self.board[height - 2][width - 2] = 'H2'



  #Initialize map
  def show_map(self):
    # Initialize resources and home base
    print('Please check the battlefield, commander.')
    for i in range(width):
        if i == 0:
          print(f'  X0{i}', end=' ')
        elif i == width - 1:
          print(f'0{i}X')
        else:
          print(f"0{i}", end=' ')
    if self.width == 5:
      line = ' Y+--------------+'
    if self.width == 6:
      line = ' Y+-----------------+'
    if self.width == 7:
      line = ' Y+--------------------+'
    print(line)
    for i in range(self.height):
      print("0{}|{}|{}|{}|{}|{}|".format(i, self.board[0][i], self.board[1][i], self.board[2][i], self.board[3][i],self.board[4][i]))
    return line



#Class Army
class Army:
  def __init__(self,price,step): 
    self.price = price
    self.step = step



#Class base
class Player:
  def __init__(self, number, base):
    self.number = number
    self.base = base
    self.army_box = {}
    self.resource_box = {'wood':2, 'food':2, 'gold':2}

  def add_army(self, army, position):
    if self.army_box.get(army) == None:
      self.army_box[army] = []
      self.army_box[army].append(position)
    else:
      self.army_box[army].append(position)
  
  def remove_army(self, army, position):
    self.army_box[army].remove(position)


  def add_resource(self, resource, number):
    self.resource_box[resource] += number

  def remove_resource(self, resource, number):
    self.resource_box[resource] -= number
 
  def show_resource(self):
      return f"[Your Asset: Wood - {self.resource_box['wood']} Food - {self.resource_box['food']} Gold - {self.resource_box['gold']}]"



 #class Game:Control the all procdedure
class Game:
  def __init__(self):
    self.board = Map()
    self.spearman = Army({'wood':1, 'food':1}, 1)
    self.archer = Army({'wood':1, 'gold':1}, 1)
    self.knight = Army({'food':1, 'gold':1}, 1)
    self.scout = Army({'wood':1, 'gold':1, 'food':1}, 1)
  

  #staticmethod
  def show_price():
    return '''Recruit Prices:
  Spearman (S) - 1W, 1F
  Archer (A) - 1W, 1G
  Knight (K) - 1F, 1G
  Scout (T) - 1W, 1F, 1G'''



 ### Recruit Part!!!
  def recruit_army(self,player1, playr2):
    ## play's turn 
    if step % 2 == 0:
      current_player, next_player = (player1, player2)
    else:
      current_player, next_player = (player2, player1)

    print(f"+++Player {current_player.number}'s Stage: Recruit Armies+++\n")
    flag1 = True                                                                                                          
    while flag1:
      print(current_player.show_resource())
      if ((current_player.resource_box['wood'] == 0 and current_player.resource_box['food'] == 0 and current_player.resource_box['gold'] == 0) or
          (current_player.resource_box['food'] == 0 and current_player.resource_box['gold'] == 0) or
          (current_player.resource_box['wood'] == 0 and current_player.resource_box['gold'] == 0) or
          (current_player.resource_box['wood'] == 0 and current_player.resource_box['food'] == 0)):
          return 'No resources to recruit any armies.\n'
      if (self.board.board[current_player.base[0]+1][current_player.base[1]] != '  ' and
          self.board.board[current_player.base[0]-1][current_player.base[1]] != '  ' and
          self.board.board[current_player.base[0]][current_player.base[1]+1] != '  ' and
          self.board.board[current_player.base[0]][current_player.base[1]-1] != '  '):
          return 'No place to recruit any armies.\n'
     
  
      while True:
        recruit = input("\nWhich type of army to recruit, (enter) ‘S’, ‘A’, ‘K’, or ‘T’? Enter ‘NO’ to end this stage.\n")
        if recruit == 'QUIT':
          sys.exit()
        elif recruit == 'DIS':
          print(self.board.show_map())
          continue
        elif recruit == 'PRIS':
          print(Game.show_price())
          continue  
        elif recruit == 'NO':
          return ''
        elif recruit != 'S' and recruit != 'A' and recruit != 'K' and recruit != 'T':
          print('Sorry, invalid input. Try again.') 


    #check the resource whether available
        if recruit == 'S':
          army = 'Spearman'
          wood_count = self.spearman.price['wood']
          food_count = self.spearman.price['food']
          if current_player.resource_box['wood'] < wood_count or current_player.resource_box['food'] < food_count:
            print('Insufficient resources. Try again.')
            continue
          else:
            current_player.remove_resource('wood',1)
            current_player.remove_resource('food',1)
            break
        if recruit == 'A':
          army = 'Archer'
          wood_count = self.archer.price['wood']
          gold_count = self.archer.price['gold']
          if current_player.resource_box['wood'] < wood_count or current_player.resource_box['gold'] < gold_count:
            print('Insufficient resources. Try again.')
            continue
          else:
            current_player.remove_resource('wood',1)
            current_player.remove_resource('gold',1)
            break
        if recruit == 'K':
          army = 'Knight'
          food_count = self.knight.price['food']
          gold_count = self.knight.price['gold']
          if current_player.resource_box['food'] < food_count or current_player.resource_box['gold'] < gold_count:
            print('Insufficient resources. Try again.')
            continue
          else:
            current_player.remove_resource('food',1)
            current_player.remove_resource('gold',1)
            break
        if recruit == 'T':
          army = 'Scout'
          wood_count = self.scout.price['wood']
          food_count = self.scout.price['food']
          gold_count = self.scout.price['gold']
          if current_player.resource_box['wood'] < wood_count or current_player.resource_box['food'] < food_count or current_player.resource_box['gold'] < gold_count:
            print('Insufficient resources. Try again.') 
            continue         
          else:
            current_player.remove_resource('wood',1)
            current_player.remove_resource('food',1)
            current_player.remove_resource('gold',1)
            break
      

      while True:
        position = input(f'\nYou want to recruit a {army}. Enter two integers as format ‘x y’ to place your army.\n')
        if position == 'QUIT':
          sys.exit()
        if position == 'DIS':
          print(self.board.show_map())
          continue
        if position == 'PRIS':
          print(Game.show_price())
          continue    
        if position == 'N0':
           return ''
        if len(position) != 3 :
          print('Sorry, invalid input. Try again.')
          continue
        if not position[0].isdigit() or not position[2].isdigit() or position[1] != ' ':
            print('Sorry, invalid input. Try again.')
            continue
        row, column = position.split()  
        # position is out of the map
        if int(row) > width -1 or int(column) > height -1 :
          print('You must place your newly recruited unit in an unoccupied position next to your home base. Try again.')
          continue    
        # position is occupied
        if self.board.board[int(row)][int(column)] != '  ':
          print('You must place your newly recruited unit in an unoccupied position next to your home base. Try again.')
          continue
        if (int(row) > current_player.base[0]+1 or int(row) < current_player.base[0]-1 
            or int(column) > current_player.base[1]+1 or int(column) < current_player.base[1]-1 ):
              print('You must place your newly recruited unit in an unoccupied position next to your home base. Try again.')
              continue
        if (((int(row) == current_player.base[0] and (int(column)-1 == current_player.base[1] or int(column)+1 == current_player.base[1])) and
            self.board.board[int(row)][int(column)] == '  ')  or
            ((int(column) == current_player.base[1] and (int(row)-1 == current_player.base[0] or int(row)+1 == current_player.base[0])) and
            self.board.board[int(row)][int(column)] == '  ')):
            print(f'\nYou has recruited a {army}.\n')
            #update map
            self.board.board[int(row)][int(column)] = recruit + str(current_player.number)
            #update data
            current_player.add_army(army, (int(row), int(column)))
            break
        else:
          print('You must place your newly recruited unit in an unoccupied position next to your home base. Try again.')
          continue

    

 ###Move part!!
  def move_army(self,player1, player2):
    
    if step % 2 == 0:
      current_player, next_player = (player1, player2)
    else:
      current_player, next_player = (player2, player1)

    print(f"===Player {current_player.number}'s Stage: Move Armies===")
    army_box2 = copy.deepcopy(current_player.army_box)
    
    flag1 = True
    while flag1:
      print()
      #check if there is any army
      flag2 = True
      for army in army_box2:
        if len(army_box2[army]) != 0:
          flag2 = False
          break
      if flag2 == True:
        return 'No Army to Move: next turn.\n'
     
     # Show the army who can move 
      print('Armies to Move:')
      ls = ['Spearman', 'Archer', 'Knight', 'Scout']
      for army in ls:
        if army in army_box2:
          if len(army_box2[army]) != 0:
            if len(army_box2[army]) == 1:
              print(f'  {army}: {current_player.army_box[army][0]}')
            elif len(army_box2[army]) == 2:
              print(f'  {army}: {current_player.army_box[army][0]}, {current_player.army_box[army][1]}')
            elif len(army_box2[army]) == 3:
              print(f'  {army}: {current_player.army_box[army][0]}, {current_player.army_box[army][1]}, {current_player.army_box[army][2]}')
            elif len(army_box2[army]) == 4:
              print(f'  {army}: {current_player.army_box[army][0]}, {current_player.army_box[army][1]}, {current_player.army_box[army][2]}, {current_player.army_box[army][3]}')
            elif len(army_box2[army]) == 5:
              print(f'  {army}: {current_player.army_box[army][0]}, {current_player.army_box[army][1]}, {current_player.army_box[army][2]}, {current_player.army_box[army][3]}, {current_player.army_box[army][4]}')
      
      integer = input('\nEnter four integers as a format ‘x0 y0 x1 y1’ to represent move unit from (x0, y0) to (x1, y1) or ‘NO’ to end this turn.\n')
      if integer == 'QUIT':
        sys.exit()
      elif integer == 'DIS':
        print(self.board.show_map())
        continue
      elif integer == 'PRIS':
        print(Game.show_price())
        continue    
      elif integer == 'NO':
        return ''
      if len(integer) != 7:
        print('Invalid move. Try again.')
        continue
      if (not integer[0].isdigit() or not integer[2].isdigit or
          not integer[4].isdigit() or not integer[6].isdigit or
          integer[1] != ' ' or integer[3] != ' ' or integer[5] != ' '):
          print('Invalid move. Try again.')
          continue
     

      positionarmy = tuple(integer[0:3].replace(' ', ''))
      positionarmy = tuple(map(int, positionarmy))
      row1,column1 = positionarmy[0],positionarmy[1]
      move_position = tuple(integer[4:].replace(' ', ''))
      move_position = tuple(map(int, move_position))
      row2, column2 = move_position[0], move_position[1]
       
  #check position whether outside of map and same move position
      if row1 > width-1 or column1 > height-1 or row2 > width-1 or column2 > height-1:
        print('Invalid move. Try again.')
        continue
      elif row1 == row2 and column1 == column2 :
        print('Invalid move. Try again.')
        continue


      flag2 = False
      for army in current_player.army_box:
        if positionarmy in current_player.army_box[army]:
          flag2 = True
          army2 = army
          break

      if flag2 == False:
        print('Invalid move. Try again.')
        continue
          
      #If the army is scout,we need to have different condition
      if army2 == 'Scout':

        if (((row2 == row1 and ( column2 == column1 -2 or column2 == column1 +  2 or column2 == column1 -1 or column2 == column1 +  1 )) and
             self.board.board[row2][column2][1] != str(current_player.number)) or 
             ((column2 == column1 and ( row2 == row1 -2 or row2 == row1 + 2 or row2 == row1 -1 or row2 == row1 + 1 )) and
             (self.board.board[row2][column2][1] != str(current_player.number)))):
          
          print(f'\nYou have moved {army2} from {positionarmy} to {move_position}.')
          current_player.remove_army(army2, positionarmy)
          current_player.add_army(army2, move_position)
          army_box2[army2].remove(positionarmy)

          #check scout whether moving one step or two steps
          if  row1 == row2:
            if column2 == column1 + 2:
              row3 = row1
              column3 = column1 + 1
            elif column2 == column1 - 2:
              row3 = row1
              column3 = column1 - 1
            else:
              row3 = row2
              column3 = column2

          if column1 == column2 :
            if row2 == row1 + 2:
              column3 = column1
              row3 = row1 + 1
            elif row2 == row1 - 2:
              column3 = column1
              row3 = row1 -1
            else:
              column3 = column2
              row3 = row2
          
          # if the scout move two steps
          if row2 != row3 or column2 != column3:
              
            #check the first step for scout
              # the first step is water
            if self.board.board[row3][column3] == '~~':
              print(f"We lost the army {army2} due to your command!")
              current_player.remove_army(army2, move_position)
              self.board.board[row1][column1] = '  '
          
              #The first condition is resources or empty or friendly army
            elif (self.board.board[row3][column3] == 'WW' or #the first step is the resource
                  self.board.board[row3][column3] == 'FF' or
                  self.board.board[row3][column3] == 'GG' or
                  self.board.board[row3][column3] == '  ' or
                  self.board.board[row3][column3][1] == str(current_player.number)):
                  
                  #the first step is the resource or empty
                  if self.board.board[row3][column3] == 'WW':
                    print('Good. We collected 2 Wood.')
                    current_player.add_resource('wood', 2)
                    self.board.board[row3][column3] = '  '
                  elif self.board.board[row3][column3] == 'FF':
                    print('Good. We collected 2 Food.')
                    current_player.add_resource('food', 2)
                    self.board.board[row3][column3] = '  '
                  elif self.board.board[row3][column3] == 'GG':
                    print('Good. We collected 2 Gold.')
                    current_player.add_resource('gold', 2)
                    self.board.board[row3][column3] = '  '
              
      

                  ## check the second step for army whose first step is not enemy or water
                    # the second step is water
                  if self.board.board[row2][column2] == '~~':
                    print(f"We lost the army {army2} due to your command!")
                    current_player.remove_army(army2, move_position)
                    self.board.board[row1][column1] = '  '

                      # the second step is resources
                  elif self.board.board[row2][column2] == 'WW':
                    print('Good. We collected 2 Wood.')
                    current_player.add_resource('wood', 2)
                    self.board.board[row2][column2] = self.board.board[row1][column1]
                    self.board.board[row1][column1] = '  '
                  elif self.board.board[row2][column2] == 'FF':
                    print('Good. We collected 2 Food.')
                    self.board.board[row2][column2] = self.board.board[row1][column1]
                    self.board.board[row1][column1] = '  '
                    current_player.add_resource('food', 2)
                  elif self.board.board[row2][column2] == 'GG':
                    print('Good. We collected 2 Gold.')
                    self.board.board[row2][column2] = self.board.board[row1][column1]
                    self.board.board[row1][column1] = '  '
                    current_player.add_resource('gold', 2)
                  #the second step is empty
                  elif self.board.board[row2][column2] == '  ':
                    self.board.board[row2][column2] = self.board.board[row1][column1]
                    self.board.board[row1][column1] = '  '
                  # the second step is the enemy
                  elif (self.board.board[row2][column2][1] != str(current_player.number) and 
                        self.board.board[row2][column2][0] != 'H'):
                    if self.board.board[row2][column2][0] == 'T':
                      print('We destroyed the enemy Scout with massive loss!')
                      next_player.remove_army('Scout', move_position)
                      self.board.board[row1][column1] = '  '
                      self.board.board[row2][column2] = '  '
                    if self.board.board[row2][column2][0] == 'S':
                      print('We lost the army Scout due to your command!')
                      self.board.board[row1][column1] = '  ' 
                    if self.board.board[row2][column2][0] == 'K':
                      print('We lost the army Scout due to your command!')
                      self.board.board[row1][column1] = '  '
                    if self.board.board[row2][column2][0] == 'A':
                      print('We lost the army Scout due to your command!')
                      self.board.board[row1][column1] = '  '
                    
                    current_player.remove_army(army2, move_position) #remove army 
                  
      
                      # the second step is the base
                  elif (self.board.board[row2][column2][0] == 'H' and
                        self.board.board[row2][column2][1] != str(current_player.number)):                  
                        print(f'The army {army2} captured the enemy’s capital.\n')
                        self.board.board[row2][column2] = self.board.board[row1][column1]
                        self.board.board[row1][column1] = '  '
                        name = input('What’s your name, commander?\n')
                        print()
                        print(f'***Congratulation! Emperor {name} unified the country in {year}.***')
                        sys.exit()
                  else:
                    print('Invalid move. Try again.')
                    continue

            #the first step is the enemy 
            elif (self.board.board[row3][column3][1] != str(current_player.number) and 
                  self.board.board[row3][column3][0] != 'H'):
                  
              if self.board.board[row3][column3][0] == 'T':
                print('We destroyed the enemy Scout with massive loss!')
                next_player.remove_army('Scout', move_position)
                self.board.board[row3][column3] = '  '
                self.board.board[row1][column1] = '  '
              elif self.board.board[row3][column3][0] == 'S':
                print('We lost the army Scout due to your command!')
                self.board.board[row1][column1] = '  '
              elif self.board.board[row3][column3][0] == 'K':
                print('We lost the army Scout due to your command!')
                self.board.board[row1][column1] = '  '
              elif self.board.board[row3][column3][0] == 'A':
                print('We lost the army Scout due to your command!')
                self.board.board[row1][column1] = '  '
              current_player.remove_army(army2, move_position)

            # the first step the the army base
            elif (self.board.board[row3][column3][0] == 'H' and
                  self.board.board[row3][column3][1] != str(current_player.number)):
                print(f'The army {army2} captured the enemy’s capital.\n')
                self.board.board[row3][column3] = self.board.board[row1][column1]
                self.board.board[row1][column1] = '  '
                name = input('What’s your name, commander?\n')
                print()
                print(f'***Congratulation! Emperor {name} unified the country in {year}.***')
                sys.exit()
            else:
              print('Invalid move. Try again.')
              continue

          # if the army move only one step
          if row2 == row3 and column2 == column3:
            # position is water
            if self.board.board[row2][column2] == '~~':
              print(f"We lost the army {army2} due to your command!")
              current_player.remove_army(army2, move_position)
              self.board.board[row1][column1] = '  '

            # position is resources
            elif self.board.board[row2][column2] == 'WW':
              print('Good. We collected 2 Wood.')
              current_player.add_resource('wood', 2)
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
            elif self.board.board[row2][column2] == 'FF':
              print('Good. We collected 2 Food.')
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
              current_player.add_resource('food', 2)
            elif self.board.board[row2][column2] == 'GG':
              print('Good. We collected 2 Gold.')
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
              current_player.add_resource('gold', 2)
            #position is empty
            elif self.board.board[row2][column2] == '  ':
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
            # position is the enemy
            elif (self.board.board[row2][column2][1] != str(current_player.number) and 
                  self.board.board[row2][column2][0] != 'H'):
              if self.board.board[row2][column2][0] == 'T':
                print('We destroyed the enemy Scout with massive loss!')
                next_player.remove_army('Scout', move_position)
                self.board.board[row1][column1] = '  '
                self.board.board[row2][column2] = '  '
              if self.board.board[row2][column2][0] == 'S':
                print('We lost the army Scout due to your command!')
                self.board.board[row1][column1] = '  ' 
              if self.board.board[row2][column2][0] == 'K':
                print('We lost the army Scout due to your command!')
                self.board.board[row1][column1] = '  '
              if self.board.board[row2][column2][0] == 'A':
                print('We lost the army Scout due to your command!')
                self.board.board[row1][column1] = '  '
              
              current_player.remove_army(army2, move_position) #remove army 
                
                # position is the base
            elif (self.board.board[row2][column2][0] == 'H' and
                  self.board.board[row2][column2][1] != str(current_player.number)):                  
                  print(f'The army {army2} captured the enemy’s capital.\n')
                  self.board.board[row2][column2] = self.board.board[row1][column1]
                  self.board.board[row1][column1] = '  '
                  name = input('What’s your name, commander?\n')
                  print()
                  print(f'***Congratulation! Emperor {name} unified the country in {year}.***')
                  sys.exit()
            else:
              print('Invalid move. Try again.')
              continue
        else:
            print('Invalid move. Try again.')
            continue

      ### If the army is not the scout!!!!!!
      if army2 != 'Scout':
        if ((( row2 == row1 and ( column2 == column1 -1 or column2 == column1 +  1 )) and
            (self.board.board[row2][column2][1] != str(current_player.number))) or
            (( column2 == column1 and ( row2 == row1 -1 or row2 == row1 +  1 )) and
            (self.board.board[row2][column2][1] != str(current_player.number)))):
            
            
            print(f'\nYou have moved {army2} from {positionarmy} to {move_position}.')
            current_player.remove_army(army2, positionarmy)
            current_player.add_army(army2, move_position)
            army_box2[army2].remove(positionarmy)
            
            #if the position is resources or water
            if self.board.board[row2][column2] == '~~':
              print(f"We lost the army {army2} due to your command!")
              current_player.remove_army(army2, move_position)
              self.board.board[row1][column1] = '  '
            elif self.board.board[row2][column2] == 'WW':
              print('Good. We collected 2 Wood.')
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
              current_player.add_resource('wood', 2)
            elif self.board.board[row2][column2] == 'FF':
              print('Good. We collected 2 Food.')
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
              current_player.add_resource('food', 2)
            elif self.board.board[row2][column2] == 'GG':
              print('Good. We collected 2 Gold.')
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
              current_player.add_resource('gold', 2)
            #if the position is empty
            elif self.board.board[row2][column2] == '  ':
              self.board.board[row2][column2] = self.board.board[row1][column1]
              self.board.board[row1][column1] = '  '
            
            ##if the position is enermy
            elif self.board.board[row2][column2][0] != 'H':
              #spearman
              if self.board.board[row2][column2][0] == 'S':
                if army2 == 'Spearman':
                  print('We destroyed the enemy Spearman with massive loss!')
                  next_player.remove_army('Spearman', move_position)
                  current_player.remove_army(army2, move_position)
                  self.board.board[row2][column2] = '  '
                  self.board.board[row1][column1] = '  '
                elif army2 == 'Knight':
                  print(f'We lost the army {army2} due to your command!')
                  current_player.remove_army(army2, move_position)
                  self.board.board[row1][column1] = '  '
                elif army2 == 'Archer':
                  print('Great! We defeated the enemy Spearman!')
                  next_player.remove_army('Spearman', move_position)
                  self.board.board[row2][column2] = self.board.board[row1][column1]
                  self.board.board[row1][column1] = '  '
              #knight
              elif self.board.board[row2][column2][0] == 'K':
                if army2 == 'Spearman':
                  print('Great! We defeated the enemy Knight!')
                  next_player.remove_army('Knight', move_position)
                  self.board.board[row2][column2] = self.board.board[row1][column1]
                  self.board.board[row1][column1] = '  '
                elif army2 == 'Knight':
                  print('We destroyed the enemy Knight with massive loss!')
                  next_player.remove_army('Knight', move_position)
                  current_player.remove_army(army2, move_position)
                  self.board.board[row2][column2] = '  '
                  self.board.board[row1][column1] = '  '
                elif army2 == 'Archer':
                  print(f'We lost the army {army2} due to your command!')
                  current_player.remove_army(army2, move_position)
                  self.board.board[row1][column1] = '  '
             #archer
              elif self.board.board[row2][column2][0] == 'A':
                if army2 == 'Spearman':
                  print(f'We lost the army {army2} due to your command!')
                  current_player.remove_army(army2, move_position)
                  self.board.board[row1][column1] = '  '
                elif army2 == 'Knight':
                  print('Great! We defeated the enemy Archer!')
                  next_player.remove_army('Archer', move_position)
                  self.board.board[row2][column2] = self.board.board[row1][column1]
                  self.board.board[row1][column1] = '  '
                elif army2 == 'Archer':
                  print('We destroyed the enemy Archer with massive loss!')
                  next_player.remove_army('Archer', move_position)
                  current_player.remove_army(army2, move_position)
                  self.board.board[row2][column2] = '  '
                  self.board.board[row1][column1] = '  '
               #scout   
              elif self.board.board[row2][column2][0] == 'T':
                print('Great! We defeated the enemy Scout!')
                next_player.remove_army('Scout', move_position)
                self.board.board[row2][column2] = self.board.board[row1][column1]
                self.board.board[row1][column1] = '  '
            # the position is  the army base
            elif (self.board.board[row2][column2][0] == 'H' and 
                  self.board.board[row2][column2][1] != str(current_player.number)):
                print(f'The army {army2} captured the enemy’s capital.\n')
                name = input('What’s your name, commander?\n')
                self.board.board[row2][column2] = self.board.board[row1][column1]
                self.board.board[row1][column1] = '  '
                print()
                print(f'***Congratulation! Emperor {name} unified the country in {year}.***')
                sys.exit()
            else:
              print('Invalid move. Try again.')
              continue

          
        else:
          print('Invalid move. Try again.')
          continue
  



if __name__ == "__main__":
  if len(sys.argv) != 2:
      print("Usage: python3 little_battle.py <filepath>")
      sys.exit()
  width, height, waters, woods, foods, golds = load_config_file(sys.argv[1])
  game = Game()
  player1 = Player(1,(1,1))
  player2 = Player(2,(height-2, width -2))
  # every round needs these statements.
  def round():
    print('Game Started: Little Battle! (enter QUIT to quit the game)\n')
    print(game.board.show_map())
    print('(enter DIS to display the map)\n')
    print(Game.show_price())
    return '(enter PRIS to display the price list)\n'

  step = 0    
  year = 617
  print(round())
  ## loop the whole game
  while True:
    print(f'-Year {year}-\n')
    print(game.recruit_army(player1, player2))
    print(game.move_army(player1, player2))
    step += 1
    if step % 2 == 0:
      year += 1

