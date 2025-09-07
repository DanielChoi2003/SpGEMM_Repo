#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/array.hpp>
#include <ygm/io/csv_parser.hpp>
#include <ygm/container/bag.hpp>
#include <fstream>


struct Edge{
    int row;
    int col;
    int value;
    bool operator<(const Edge& B) const{ // does not modify the content
        if (row != B.row) return row < B.row; // first, sort by row
        if (col != B.col) return col < B.col; // if rows are equal, sort by column
        return value < B.value; // lastly sort by value
    }

    template <class Archive>
    void serialize( Archive & ar )
    {
        ar(row, col, value);
    }
};

struct coordinate{
    int row;
    int col;
};

struct edge_info{
    int first_index = -1;
    int edge_count = 0;
};

using graph_type = ygm::container::map<int, edge_info>;

// first adds edge by setting the index of the first occurring source, then increments the edge count to 1
// if not the first not, it only increments the edge count
void update_edge(graph_type& graph, int src,
              int index) {

  auto updater = [](int src, edge_info& ei, int index) {
    if(ei.first_index == -1){ // first occurrence
        ei.first_index = index;
    }
    ei.edge_count++;
  };

  graph.async_visit(src, updater, index); // creates an entry if it did not exist
}

int main(int argc, char** argv){
    
    ygm::comm world(&argc, &argv);
    static ygm::comm &s_world = world;

    int Edge_num = 0;
    if(world.rank0()){ // parse the csv header to get the number of Edges
        std::string header_file = "../data/matrix_data/as-caida-header.csv";
        std::ifstream header(header_file);

        if(!header.is_open()){
            s_world.cout0("Could not find file ", header_file);
            return -1;
        }
        std::string header_info;
        std::getline(header, header_info);
        int rows;
        int columns;
        int Edges;
        for(int i = 0; i < 3; i++){
            int index;
            if(i == 0){
                index = header_info.find(',');
                rows = std::stoi(header_info.substr(0, index));
                header_info = header_info.substr(index+1);
            }
            else if(i == 1){
                index = header_info.find(',');
                columns = std::stoi(header_info.substr(0, index));
                header_info = header_info.substr(index+1);
            }
            else{
                Edges = std::stoi(header_info);
            }
        }
        header.close();

        s_world.async_bcast([&](int number_of_Edges){
            Edge_num = number_of_Edges;
        }, Edges);
    }
    world.barrier();

    //world.cout("Got number of Edges: ", Edge_num);


     // Task 1: data extraction
    ygm::container::bag<Edge> bag_A(world);
    std::vector<std::string> filename_A = {"../data/matrix_data/as-caida.csv"};
    ygm::io::csv_parser parser_A(world, filename_A);
    parser_A.for_all([&](ygm::io::detail::csv_line line){ // currently rank 0 is the only one running. is byte partition fixed?

        int row = line[0].as_integer();
        int col = line[1].as_integer();
        int value = line[2].as_integer();
        // long long vertex_one = std::min(vertex_a, vertex_b);
        // long long vertex_two = std::max(vertex_a, vertex_b);
        
        Edge ed = {row, col, value};
        bag_A.async_insert(ed);
    });

    // matrix B data extraction
    ygm::container::bag<Edge> bag_B(world);
    std::vector<std::string> filename_B = {"../data/matrix_data/as-caida.csv"};
    ygm::io::csv_parser parser_B(world, filename_B);
    parser_B.for_all([&](ygm::io::detail::csv_line line){

        int row = line[0].as_integer();
        int col = line[1].as_integer();
        int value = line[2].as_integer();
        // long long vertex_one = std::min(vertex_a, vertex_b);
        // long long vertex_two = std::max(vertex_a, vertex_b);
        
        Edge ed = {row, col, value};
        bag_B.async_insert(ed);
    });


    // Task 2: data storage and sharing among ranks
    /*
        How to split data among ranks?
        1. Using sorted Edge list for matrix A. 

            Use ygm::array, but the downside is that when it resizes, it needs to redo the partitioning.

        2. GLOBAL data structure is needed to know two things
            a. the index of the first source
            b. how many edges with that source (so I don't have to calculate how many times to iterate later)
        
        3. (optional) Using sorted Edge list for matrix B. Must be tested whether this brings performance boost.

        Questions: 
            1. Is there a better way to partition csv data among ranks than inserting them into a bag then into an array?
            2. How does array globally sort? similar to merge sort?
            3. Currently, only rank 1 is performing the csv parse. Is it because due to byte fixed partitioning?
            4. how to deallocate the bag containers?
    */

    ygm::container::map<int, edge_info> edge_book(world); // source, <first index, edge count>
    ygm::container::array<Edge> matrix_A(world, bag_A);
    matrix_A.sort(); // Globally sort matrix A
    ygm::container::array<Edge> matrix_B(world, bag_B); // BOOKMARK: Sort matrix B later
    // deallocate bag_A and bag_B
    bag_A.clear();
    bag_B.clear();

    matrix_A.for_all([&edge_book](int index, Edge &ed){

        // insert if it did not exist before
        // increment the edge count if it has already existed
        update_edge(edge_book, ed.row, index);
    });
    //world.cout("I got ", i, " lines");

    edge_book.for_all([](int src, edge_info &ei){

        s_world.cout("Source: ", src, ", first index: ", ei.first_index, ", number of edges with the source: ", ei.edge_count);
    });
    /*
        Task 3: matrix C data structure

        1. Naive implementation: use ygm::map
    */
   ygm::container::map<coordinate, int> matrix_C(world);  // <row, col>, partial product 

    /*
        Task 4: perform outer product multiplication

        1. matrix B initiates the multiplication

        2. Find the rank that holds the first occurrence of the matching row (col B == row A)
        
    */

    matrix_B.for_all([&matrix_A](int index, Edge &ed){

        int column_B = ed.col;
        int first_index;

        // auto multiplier = [](int index, Edge &ed, int ){

        // };

        //matrix_A.async_visit();
    });

    /*
        Task 5: Storing the partial products

        1. How to store partial products?
            a. create a linked list off the same key
            b. use mapped_reduce() if the key already exists (overwriting)
    */




}