#set term pos eps font 20 color colortext
set term pdf enhanced
set output "speculation.pdf"
set title "Speculation Happen"
#set key below autotitle columnheader

set grid
set xlabel "Slow task percentage"
set ylabel "Slow task bandwidth"

plot [0:100] [0:5] 'spec.txt' with points lt 3 pt 4, 'nospec.txt' with points lt 1 pt 3

