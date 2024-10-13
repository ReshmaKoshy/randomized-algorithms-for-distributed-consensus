use "time"
use "collections"
use "random"

trait Node
  be add_neighbors(neighbors_list: Array[Node tag] val)
  be hear_message(sourceNode: Node tag, message: (None | (F64 val, F64 val)))
  be remove_terminated_neighbor(sourceNode: Node tag)
  fun ref send_to_neighbors()

actor Main
  let _env: Env
  let network: Array[Node tag] val
  var finished_count: U64 = 0
  let num_nodes: U64
  var protocol_start: U64 = 0
  let topology: String
  let algorithm: String
  let rand: Rand = Rand

  new create(env: Env) =>
    _env = env
    num_nodes = try env.args(2)?.u64()? else 27 end
    topology = try env.args(3)? else "3D" end
    algorithm = try env.args(4)? else "gossip" end

    //createNetwork
    network = recover val
      let arr = Array[Node tag]
      for i in Range[U64](0, num_nodes) do
        if algorithm == "gossip" then
          arr.push(GossipNode(_env, i, this))
        else
          arr.push(PushSumNode(_env, i, this))
        end
      end
      arr
    end

    // Assign neighbors based on topology
    match topology
    | "3D" =>
      let cube_side = num_nodes.f64().pow(1.0/3.0).round().usize()
      assign_3D_connected_neighbors(cube_side)
    | "full" => assign_full_connected_neighbors()
    | "line" => assign_line_connected_neighbors()
    | "imp3D" =>
      let cube_side = num_nodes.f64().pow(1.0/3.0).round().usize()
      assign_imp3D_connected_neighbors(cube_side)
    else
      _env.out.print("Unknown topology. Using 3D.")
      let cube_side = num_nodes.f64().pow(1.0/3.0).round().usize()
      assign_3D_connected_neighbors(cube_side)
    end

    _env.out.print("Starting " + algorithm + " protocol...")
    protocol_start = Time.micros()

    let first_node = try network(rand.int(network.size().u64()).usize())? else _env.out.print("Index of first_node is out of bounds! Exiting program."); return end
    first_node.hear_message(if algorithm == "gossip" then GossipNode(_env, 0, this) else PushSumNode(_env, 0, this) end, None)

  fun ref assign_full_connected_neighbors() =>
    for i in Range[USize](0, network.size()) do
      let neighbors = recover val
        let arr = Array[Node tag]
        for j in Range[USize](0, network.size()) do
          if i != j then
            try arr.push(network(j)?) end
          end
        end
        consume arr
      end
      try network(i)?.add_neighbors(neighbors) end
    end

  fun ref assign_line_connected_neighbors() =>
  
    for i in Range[USize](0, network.size()) do
      let neighbors = recover val
        let arr = Array[Node tag]
        if i > 0 then try arr.push(network(i-1)?) end end
        if i < (network.size() - 1) then try arr.push(network(i+1)?) end end
        consume arr
      end
      try network(i)?.add_neighbors(neighbors) end
    end

    // Assign neighbors in a 3D grid topology
  fun assign_3D_connected_neighbors(cube_side: USize) =>
    for i in Range[USize](0, network.size()) do
      let x = i % cube_side
      let y = (i / cube_side) % cube_side
      let z = i / (cube_side * cube_side)
      let neighbors = recover val
          let arr = Array[Node tag]
          
          try

            if x > 0 then arr.push(network(i - 1)?) end  // left neighbor
            if x < (cube_side - 1) then arr.push(network(i + 1)?) end  // right neighbor
            if y > 0 then arr.push(network(i - cube_side)?) end  // up neighbor
            if y < (cube_side - 1) then arr.push(network(i + cube_side)?) end  // down neighbor
            
            // Adding neighbors in the next and previous planes (z-axis) in a 3D grid
            if z > 0 then arr.push(network(i - (cube_side * cube_side))?) end  // previous plane
            if z < (cube_side - 1) then arr.push(network(i + (cube_side * cube_side))?) end  // next plane
            
            consume arr
          else
            _env.out.print("Index " + i.string() + " is out of bounds! Exiting program.")
            return
          end
        end
      try network(i)?.add_neighbors(neighbors) else _env.out.print("Index " + i.string() + " is out of bounds! Exiting program."); return end
    end

  fun assign_imp3D_connected_neighbors(cube_side: USize) =>
    for i in Range[USize](0, network.size()) do
      let x = i % cube_side
      let y = (i / cube_side) % cube_side
      let z = i / (cube_side * cube_side)
      let neighbor_idx_arr:  Array[USize] = [(i-1) ; (i+1); (i-cube_side); (i+cube_side); (i - (cube_side * cube_side)); (i + (cube_side * cube_side)) ]
      let neighbors = recover val
          let arr = Array[Node tag]
          
          try

            if x > 0 then arr.push(network(i - 1)?) end  // left neighbor
            if x < (cube_side - 1) then arr.push(network(i + 1)?) end  // right neighbor
            if y > 0 then arr.push(network(i - cube_side)?) end  // up neighbor
            if y < (cube_side - 1) then arr.push(network(i + cube_side)?) end  // down neighbor
            
            // Adding neighbors in the next and previous planes (z-axis) in a 3D grid
            if z > 0 then arr.push(network(i - (cube_side * cube_side))?) end  // previous plane
            if z < (cube_side - 1) then arr.push(network(i + (cube_side * cube_side))?) end  // next plane
            
            let rand_itr = Rand
            var random_neighbor_index: USize = rand_itr.int(num_nodes).usize()
            while neighbor_idx_arr.contains(random_neighbor_index) or (i==random_neighbor_index) do 
              random_neighbor_index = rand_itr.int(num_nodes).usize()
            end
            arr.push(network(random_neighbor_index)?) 

            consume arr
          else
            return
          end
        end
      try network(i)?.add_neighbors(neighbors) else _env.out.print("Index " + i.string() + " is out of bounds! Exiting program."); return end
    end
  
  be node_terminated(nodeId: U64, message: (None | F64 val)) =>
    finished_count = finished_count + 1
    match algorithm
      |"gossip" => _env.out.print(nodeId.string() + " heard message (10) times and finished gossiping")
      else 
        match message
          | let converged_s_w_ratio : F64 val => _env.out.print(nodeId.string() + " converged with s_w_ratio: " + converged_s_w_ratio.string())
        end
    end
    if finished_count == num_nodes then
      _env.out.print("Time to finish " + algorithm + " protocol with " + num_nodes.string() + " nodes, " + topology + " topology is " + (Time.micros() - protocol_start).string() + " (microseconds)")
    end

actor GossipNode is Node
  let id: U64
  var rumor_count: U64 = 0
  var terminated: Bool = false
  let neighbors: Array[Node tag] = Array[Node tag]
  let main: Main tag
  let _env: Env
  let rand: Rand

  new create(env: Env, id': U64, main': Main tag) =>
    id = id'
    main = main'
    _env = env
    rand = Rand(id')

  be add_neighbors(neighbors_list: Array[Node tag] val) =>
    for neighbor in neighbors_list.values() do
      neighbors.push(neighbor)
    end

  be hear_message(sourceNode: Node tag, message: (None | (F64 val, F64 val))) =>
    if rumor_count < 10 then
      if not (this is sourceNode) then
        rumor_count = rumor_count + 1
      end
      send_to_neighbors()
    elseif not terminated then
      terminated = true
      main.node_terminated(id, None)
    else
      sourceNode.remove_terminated_neighbor(this)
    end

  fun ref send_to_neighbors() =>
    for _ in Range[U64](0, (neighbors.size().f64()/3).ceil().u64()) do
      if neighbors.size() > 0 then
        try
          let random_neighbor = neighbors(rand.int(neighbors.size().u64()).usize())?
          random_neighbor.hear_message(this, None)
        end
      end
    end
    this.hear_message(this, None)

  be remove_terminated_neighbor(sourceNode: Node tag) =>
    try
      let idx = neighbors.find(sourceNode)?
      neighbors.delete(idx)?
    end

actor PushSumNode is Node
  let id: U64
  let neighbors: Array[Node tag] = Array[Node tag]
  let main: Main tag
  let _env: Env
  let rand: Rand
  var s: F64 = 0.0
  var w: F64  = 1.0
  var s_w_ratio: F64 = 0.0
  var round_count: U64 = 0

  new create(env: Env, id': U64, main1': Main tag) =>
    id = id'
    s=id'.f64()
    main = main1'
    _env = env
    rand = Rand(id')
    s_w_ratio = s/w

  be add_neighbors(neighbors_list: Array[Node tag] val) =>
    for neighbor in neighbors_list.values() do
      neighbors.push(neighbor)
    end

  be hear_message(sourceNode: Node tag, message: (None | (F64 val, F64 val))) =>
    if round_count<3 then
      if not (this is sourceNode) then
        match message
          |(let neighbor_s: F64 val, let neighbor_w: F64 val) => 
            s = s + neighbor_s
            w = w + neighbor_w
            let curr_ratio = s/w
            if (curr_ratio - s_w_ratio).abs() < 1e-10 then
              round_count = round_count + 1
            else round_count = 0
            end
            s_w_ratio = curr_ratio
            if round_count == 3 then main.node_terminated(id, s_w_ratio) end
        end
      end
      send_to_neighbors()
    else
      sourceNode.remove_terminated_neighbor(this)
    end

  fun ref send_to_neighbors() =>
    for _ in Range[U64](0, (neighbors.size().f64()/3).ceil().u64()) do
      if neighbors.size() > 0 then
        try
          s=s/2
          w=w/2
          let random_neighbor = neighbors(rand.int(neighbors.size().u64()).usize())?
          random_neighbor.hear_message(this, (s,w))
        end
      end
    end
    this.hear_message(this, None)

  be remove_terminated_neighbor(sourceNode: Node tag) =>
    try
      let idx = neighbors.find(sourceNode)?
      neighbors.delete(idx)?
    end
