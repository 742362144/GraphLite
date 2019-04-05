/**
 * @file PageRankVertex.cc
 * @author  Songjie Niu, Shimin Chen
 * @version 0.1
 *
 * @section LICENSE
 *
 * Copyright 2016 Shimin Chen (chensm@ict.ac.cn) and
 * Songjie Niu (niusongjie@ict.ac.cn)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @section DESCRIPTION
 *
 * This file implements the PageRank algorithm using graphlite API.
 *
 */

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <map>
#include <set>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) PageRankVertex##name

#define EPS 1e-6

#define IN_NEIGHBOR 1
#define OUT_NEIGHBOR 2

struct MyMsg {
	int64_t vid;
	int64_t neighbor;
	int64_t type;  // 标记出度或者入度
};
struct Counter {
	int64_t in = 0;
	int64_t out = 0;
	int64_t through = 0;
	int64_t cycle = 0;
};

class VERTEX_CLASS_NAME(InputFormatter) : public InputFormatter {
public:
	int64_t getVertexNum() {
		unsigned long long n;
		sscanf(m_ptotal_vertex_line, "%lld", &n);
		m_total_vertex = n;
		return m_total_vertex;
	}
	int64_t getEdgeNum() {
		unsigned long long n;
		sscanf(m_ptotal_edge_line, "%lld", &n);
		m_total_edge = n;
		return m_total_edge;
	}
	int getVertexValueSize() {
		m_n_value_size = sizeof(double);
		return m_n_value_size;
	}
	int getEdgeValueSize() {
		m_e_value_size = sizeof(double);
		return m_e_value_size;
	}
	int getMessageValueSize() {
		m_m_value_size = sizeof(double);
		return m_m_value_size;
	}
	void loadGraph() {
		if (m_total_edge <= 0)  return;

		unsigned long long last_vertex;
		unsigned long long from;
		unsigned long long to;
		double weight = 0;

		double value = 1;
		int outdegree = 0;

		const char *line = getEdgeLine();

		// Note: modify this if an edge weight is to be read
		//       modify the 'weight' variable

		sscanf(line, "%lld %lld", &from, &to);
		addEdge(from, to, &weight);

		last_vertex = from;
		++outdegree;
		for (int64_t i = 1; i < m_total_edge; ++i) {
			line = getEdgeLine();

			// Note: modify this if an edge weight is to be read
			//       modify the 'weight' variable

			sscanf(line, "%lld %lld", &from, &to);
			if (last_vertex != from) {
				addVertex(last_vertex, &value, outdegree);
				last_vertex = from;
				outdegree = 1;
			}
			else {
				++outdegree;
			}
			addEdge(from, to, &weight);
		}
		addVertex(last_vertex, &value, outdegree);
	}
};

class VERTEX_CLASS_NAME(OutputFormatter) : public OutputFormatter {
public:
	void writeResult() {
		int64_t vid;
		double value;
		char s[1024];

		for (ResultIterator r_iter; !r_iter.done(); r_iter.next()) {
			r_iter.getIdValue(vid, &value);
			int n = sprintf(s, "%lld: %f\n", (unsigned long long)vid, value);
			writeNextResLine(s, n);
		}
	}
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator) : public Aggregator<int64_t> {
public:
	void init() {
		m_global = 0;
		m_local = 0;
	}
	void* getGlobal() {
		return &m_global;
	}
	void setGlobal(const void* p) {
		m_global = *(int64_t *)p;
	}
	void* getLocal() {
		return &m_local;
	}
	void merge(const void* p) {
		m_global += *(int64_t *)p;
	}
	void accumulate(const void* p) {
		m_local += *(int64_t *)p;
	}
};

class VERTEX_CLASS_NAME() : public Vertex <Counter, double, MyMsg> {
public:
	// TODO
	// in A自身都可以检测出来
	// out A检测不出来，B和C可以检测出来，交给其他节点使用
	// through A可以检测出来
	// cycle A自身可以检测出来
	void compute(MessageIterator* pmsgs) {
		map<int64_t, set<int64_t> > in;
		map<int64_t, set<int64_t> > out;
		set<int64_t> vids;
		set<int64_t> in_neighbors;
		Counter counter;
		if (getSuperstep() == 0) {
			counter.in = 100;
			counter.out = 100;
			counter.through = 100;
			counter.cycle = 100;
		}
		else {
			if (getSuperstep() >= 50) {
				int64_t global_val = *(int64_t *)getAggrGlobal(0);
				// 总体误差小于EPS时推出
				if (global_val == 0) {
					voteToHalt(); return;
				}
			}

			// 遍历所有消息，获取已知的所有邻居的in-neighbor和out-neighbor
			for (; !pmsgs->done(); pmsgs->next()) {
				MyMsg* pm = (MyMsg*)pmsgs;
				// 统计所有的in neighbors
				in_neighbors.insert(pm->vid);
				vids.insert(pm->vid);
				if (pm->type == IN_NEIGHBOR) {
					if (in.find(pm->vid) != in.end()) {
						in[pm->vid].insert(pm->neighbor);
					}
					else {
						set<int64_t> ns;
						ns.insert(pm->neighbor);
						in.insert(make_pair(pm->vid, ns));
					}
				}
				else if (pm->type == OUT_NEIGHBOR) {
					if (out.find(pm->vid) != out.end()) {
						out[pm->vid].insert(pm->neighbor);
					}
					else {
						set<int64_t> ns;
						ns.insert(pm->neighbor);
						out.insert(make_pair(pm->vid, ns));
					}
				}
			}

			for (set<int64_t>::iterator ait = in_neighbors.begin(); ait != in_neighbors.end(); ait++) {
				int64_t ai = *ait;
				in_neighbors.erase(ait);
				for (set<int64_t>::iterator bit = in_neighbors.begin(); bit != in_neighbors.end(); bit++) {
					int64_t bi = *bit;
					// A，两个in-neighbor有通路 A包含了B，当出现A时增加B
					if (out[ai].find(bi) != out[ai].end()) {
						counter.in++;
						counter.out++;
					}
					if (out[bi].find(ai) != out[bi].end()) {
						counter.in++;
						counter.out++;
					}
				}
				OutEdgeIterator eit = getOutEdgeIterator();
				for (; !eit.done(); eit.next()) {
					if (out[ai].find(eit.target()) != out[ai].end()) {  // C in-neigbor和自身有相同的出度    
						counter.through++;
					}
					else if (in[ai].find(eit.target()) != in[ai].end()) {  // D in-neighbor的入度等于自身的出度
						counter.cycle++;
					}
				}
			}

			// 误差累积
			int64_t acc = abs(getValue().in - counter.in)
				+ abs(getValue().out - counter.out)
				+ abs(getValue().through - counter.through)
				+ abs(getValue().cycle - counter.cycle);
			accumulateAggr(0, &acc);
		}
		// val就是本节点的rank，更新
		mutableValue()->in = counter.in;
		mutableValue()->out = counter.out;
		mutableValue()->through = counter.through;
		mutableValue()->cycle = counter.cycle;

		// send msg to all outEdge
		printf("%lld %lld %lld %lld\n", counter.in, counter.out, counter.through, counter.cycle);
		OutEdgeIterator eit = getOutEdgeIterator();
		for (; !eit.done(); eit.next()) {
			MyMsg m;
			m.neighbor = eit.target();
			m.type = OUT_NEIGHBOR;
			m.vid = getVertexId();
			sendMessageToAllNeighbors(m);
		}
		// 对out遍历，取出所有的vid
		for (set<int64_t>::iterator it = vids.begin(); it != vids.end(); it++) {
			MyMsg m;
			m.neighbor = *it;
			m.type = IN_NEIGHBOR;
			m.vid = getVertexId();
			sendMessageToAllNeighbors(m);
		}
	}
};

class VERTEX_CLASS_NAME(Graph) : public Graph {
public:
	VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
	// argv[0]: PageRankVertex.so
	// argv[1]: <input path>
	// argv[2]: <output path>
	void init(int argc, char* argv[]) {

		setNumHosts(5);
		setHost(0, "localhost", 1411);
		setHost(1, "localhost", 1421);
		setHost(2, "localhost", 1431);
		setHost(3, "localhost", 1441);
		setHost(4, "localhost", 1451);

		if (argc < 3) {
			printf("Usage: %s <input path> <output path>\n", argv[0]);
			exit(1);
		}

		m_pin_path = argv[1];
		m_pout_path = argv[2];

		aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
		regNumAggr(1);
		regAggr(0, &aggregator[0]);
	}

	void term() {
		delete[] aggregator;
	}
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
	Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

	pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
	pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
	pgraph->m_pver_base = new VERTEX_CLASS_NAME();

	return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
	delete (VERTEX_CLASS_NAME()*)(pobject->m_pver_base);
	delete (VERTEX_CLASS_NAME(OutputFormatter)*)(pobject->m_pout_formatter);
	delete (VERTEX_CLASS_NAME(InputFormatter)*)(pobject->m_pin_formatter);
	delete (VERTEX_CLASS_NAME(Graph)*)pobject;
}
