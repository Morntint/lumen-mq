use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::codec::QoS;
use crate::utils::{BrokerError, BrokerResult};

/// 共享订阅前缀
const SHARE_PREFIX: &str = "$share";

/// 订阅树节点
#[derive(Default)]
struct Node {
    /// 该层级精确匹配的子节点
    children: HashMap<String, Node>,
    /// `+` 通配符子节点
    plus_child: Option<Box<Node>>,
    /// `#` 通配符子节点（匹配剩余所有层级）
    hash_child: Option<Box<Node>>,
    /// 订阅了到达此节点路径的订阅者
    subscribers: HashMap<String, QoS>,
}

impl Node {
    fn child_for_segment_mut(&mut self, seg: &str) -> &mut Node {
        match seg {
            "+" => self.plus_child.get_or_insert_with(|| Box::new(Node::default())),
            "#" => self.hash_child.get_or_insert_with(|| Box::new(Node::default())),
            _ => self.children.entry(seg.to_string()).or_default(),
        }
    }
}

/// 共享订阅条目：组成员表 + 轮询计数器
#[derive(Default)]
struct SharedEntry {
    /// client_id -> qos
    members: HashMap<String, QoS>,
    /// 轮询投递计数器（取模成员数选择）
    rr_counter: u64,
}

/// 主题订阅树，支持 `+` / `#` 通配符 + `$share/{group}/{filter}` 共享订阅
pub struct SubscriptionTree {
    root: RwLock<Node>,
    /// 反向索引：client_id → 该客户端的全部订阅 (filter, qos)
    /// 用于 O(1) 快照（持久化）与 unsubscribe_all
    reverse: RwLock<HashMap<String, Vec<(String, QoS)>>>,
    /// 共享订阅：(group, filter) → SharedEntry
    /// key 为 (group_name, real_filter)，real_filter 已剥离 $share/{group}/ 前缀
    shared: RwLock<HashMap<(String, String), SharedEntry>>,
}

impl Default for SubscriptionTree {
    fn default() -> Self {
        Self {
            root: RwLock::new(Node::default()),
            reverse: RwLock::new(HashMap::new()),
            shared: RwLock::new(HashMap::new()),
        }
    }
}

impl SubscriptionTree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, client_id: &str, filter: &str, qos: QoS) -> BrokerResult<()> {
        // 检测共享订阅：$share/{group}/{real_filter}
        if let Some((group, real_filter)) = parse_shared_filter(filter) {
            validate_filter(&real_filter)?;
            let mut shared = self.shared.write();
            let entry = shared
                .entry((group.to_string(), real_filter.to_string()))
                .or_default();
            entry.members.insert(client_id.to_string(), qos);
            // 维护反向索引（存完整 filter 字符串，便于 unsubscribe 定位）
            let mut rev = self.reverse.write();
            let list = rev.entry(client_id.to_string()).or_default();
            if let Some(slot) = list.iter_mut().find(|(f, _)| f == filter) {
                slot.1 = qos;
            } else {
                list.push((filter.to_string(), qos));
            }
            return Ok(());
        }

        validate_filter(filter)?;
        let segments: Vec<&str> = filter.split('/').collect();
        let mut root = self.root.write();
        let mut node = &mut *root;
        for seg in &segments {
            node = node.child_for_segment_mut(seg);
        }
        node.subscribers.insert(client_id.to_string(), qos);
        // 维护反向索引
        let mut rev = self.reverse.write();
        let entry = rev.entry(client_id.to_string()).or_default();
        // 若已存在同 filter，更新 qos；否则追加
        if let Some(slot) = entry.iter_mut().find(|(f, _)| f == filter) {
            slot.1 = qos;
        } else {
            entry.push((filter.to_string(), qos));
        }
        Ok(())
    }

    pub fn unsubscribe(&self, client_id: &str, filter: &str) -> BrokerResult<bool> {
        // 共享订阅
        if let Some((group, real_filter)) = parse_shared_filter(filter) {
            let mut shared = self.shared.write();
            let mut removed = false;
            if let Some(entry) = shared.get_mut(&(group.to_string(), real_filter.to_string())) {
                removed = entry.members.remove(client_id).is_some();
                // 组空则清理整个条目
                if removed && entry.members.is_empty() {
                    shared.remove(&(group.to_string(), real_filter.to_string()));
                }
            }
            if removed {
                let mut rev = self.reverse.write();
                if let Some(list) = rev.get_mut(client_id) {
                    list.retain(|(f, _)| f != filter);
                    if list.is_empty() {
                        rev.remove(client_id);
                    }
                }
            }
            return Ok(removed);
        }

        let segments: Vec<&str> = filter.split('/').collect();
        let mut root = self.root.write();
        let removed = remove_recursive(&mut root, &segments, 0, client_id);
        if removed {
            // 维护反向索引
            let mut rev = self.reverse.write();
            if let Some(entry) = rev.get_mut(client_id) {
                entry.retain(|(f, _)| f != filter);
                if entry.is_empty() {
                    rev.remove(client_id);
                }
            }
        }
        Ok(removed)
    }

    /// 移除该客户端所有订阅（含共享）
    pub fn unsubscribe_all(&self, client_id: &str) {
        // 收集该客户端的共享订阅 key
        let shared_keys: Vec<(String, String)> = {
            let shared = self.shared.read();
            shared
                .iter()
                .filter_map(|(k, e)| {
                    if e.members.contains_key(client_id) {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        // 从共享结构中移除
        if !shared_keys.is_empty() {
            let mut shared = self.shared.write();
            for k in &shared_keys {
                if let Some(entry) = shared.get_mut(k) {
                    entry.members.remove(client_id);
                    if entry.members.is_empty() {
                        shared.remove(k);
                    }
                }
            }
        }

        let mut root = self.root.write();
        remove_all(&mut root, client_id);
        // 维护反向索引
        let mut rev = self.reverse.write();
        rev.remove(client_id);
    }

    /// 匹配主题，返回去重后的 (client_id, delivery_qos)（仅普通订阅）
    pub fn matches(&self, topic: &str) -> Vec<(String, QoS)> {
        if topic.is_empty() {
            return Vec::new();
        }
        let segments: Vec<&str> = topic.split('/').collect();
        let root = self.root.read();
        let mut out: Vec<(String, QoS)> = Vec::new();
        collect_matches(&root, &segments, 0, &mut out);

        // 去重：同一客户端取最大 QoS，仅投递一次
        let mut best: HashMap<String, QoS> = HashMap::new();
        for (id, qos) in out {
            best.entry(id)
                .and_modify(|q| {
                    if qos > *q {
                        *q = qos;
                    }
                })
                .or_insert(qos);
        }
        best.into_iter().collect()
    }

    /// 匹配共享订阅：对每个 (group, filter) 组，若 topic 匹配 filter，
    /// 则在组内轮询选一个成员投递。返回选中的 (client_id, qos) 列表。
    pub fn matches_shared(&self, topic: &str) -> Vec<(String, QoS)> {
        if topic.is_empty() {
            return Vec::new();
        }
        let mut shared = self.shared.write();
        let mut selected: Vec<(String, QoS)> = Vec::new();

        for ((_, filter), entry) in shared.iter_mut() {
            if !topic_matches_filter(topic, filter) {
                continue;
            }
            if entry.members.is_empty() {
                continue;
            }
            // 收集成员并排序以保证轮询确定性
            let mut members: Vec<(&String, &QoS)> = entry.members.iter().collect();
            members.sort_by(|a, b| a.0.cmp(b.0));
            let idx = (entry.rr_counter % members.len() as u64) as usize;
            entry.rr_counter = entry.rr_counter.wrapping_add(1);
            let (cid, qos) = members[idx];
            selected.push((cid.clone(), *qos));
        }
        selected
    }

    pub fn subscriber_count(&self) -> usize {
        let root = self.root.read();
        let mut n = count_subscribers(&root);
        let shared = self.shared.read();
        for e in shared.values() {
            n += e.members.len();
        }
        n
    }

    /// 获取某客户端的全部订阅 (filter, qos)，用于持久化快照
    pub fn subscriptions_of(&self, client_id: &str) -> Vec<(String, QoS)> {
        self.reverse.read().get(client_id).cloned().unwrap_or_default()
    }
}

/// 解析共享订阅过滤器：`$share/{group}/{real_filter}` → (group, real_filter)
/// 非共享订阅返回 None
fn parse_shared_filter(filter: &str) -> Option<(&str, &str)> {
    let segs: Vec<&str> = filter.splitn(3, '/').collect();
    if segs.len() == 3 && segs[0] == SHARE_PREFIX && !segs[1].is_empty() {
        Some((segs[1], segs[2]))
    } else {
        None
    }
}

fn collect_matches(node: &Node, segs: &[&str], idx: usize, out: &mut Vec<(String, QoS)>) {
    // `#` 子节点匹配剩余所有层级
    if let Some(hash) = &node.hash_child {
        for (id, qos) in &hash.subscribers {
            out.push((id.clone(), *qos));
        }
    }
    if idx == segs.len() {
        for (id, qos) in &node.subscribers {
            out.push((id.clone(), *qos));
        }
        return;
    }
    let seg = segs[idx];
    if let Some(child) = node.children.get(seg) {
        collect_matches(child, segs, idx + 1, out);
    }
    if let Some(plus) = &node.plus_child {
        collect_matches(plus, segs, idx + 1, out);
    }
}

fn remove_recursive(node: &mut Node, segs: &[&str], idx: usize, client_id: &str) -> bool {
    if idx == segs.len() {
        return node.subscribers.remove(client_id).is_some();
    }
    let seg = segs[idx];
    let removed = match seg {
        "+" => node
            .plus_child
            .as_mut()
            .map(|c| remove_recursive(c, segs, idx + 1, client_id))
            .unwrap_or(false),
        "#" => node
            .hash_child
            .as_mut()
            .map(|c| remove_recursive(c, segs, idx + 1, client_id))
            .unwrap_or(false),
        s => node
            .children
            .get_mut(s)
            .map(|c| remove_recursive(c, segs, idx + 1, client_id))
            .unwrap_or(false),
    };
    // 清理空子节点（保守清理，避免内存增长）
    if removed {
        if let "+" = seg {
            if let Some(c) = &node.plus_child {
                if is_empty(c) {
                    node.plus_child = None;
                }
            }
        }
    }
    removed
}

fn remove_all(node: &mut Node, client_id: &str) {
    node.subscribers.remove(client_id);
    for c in node.children.values_mut() {
        remove_all(c, client_id);
    }
    if let Some(c) = node.plus_child.as_mut() {
        remove_all(c, client_id);
    }
    if let Some(c) = node.hash_child.as_mut() {
        remove_all(c, client_id);
    }
}

fn is_empty(node: &Node) -> bool {
    node.subscribers.is_empty()
        && node.children.is_empty()
        && node.plus_child.is_none()
        && node.hash_child.is_none()
}

fn count_subscribers(node: &Node) -> usize {
    let mut n = node.subscribers.len();
    for c in node.children.values() {
        n += count_subscribers(c);
    }
    if let Some(c) = &node.plus_child {
        n += count_subscribers(c);
    }
    if let Some(c) = &node.hash_child {
        n += count_subscribers(c);
    }
    n
}

/// 校验主题过滤器合法性
fn validate_filter(filter: &str) -> BrokerResult<()> {
    if filter.is_empty() {
        return Err(BrokerError::InvalidTopic("empty topic filter".into()));
    }
    // `#` 必须独占一层且为最后一层
    for (i, seg) in filter.split('/').enumerate() {
        if seg.contains('#') && seg != "#" {
            return Err(BrokerError::InvalidTopic(format!(
                "invalid '#' in filter '{filter}'"
            )));
        }
        if seg == "#" && i != filter.split('/').count() - 1 {
            return Err(BrokerError::InvalidTopic(format!(
                "'#' must be last level in '{filter}'"
            )));
        }
        // `+` 必须独占一层
        if seg.contains('+') && seg != "+" {
            return Err(BrokerError::InvalidTopic(format!(
                "invalid '+' in filter '{filter}'"
            )));
        }
    }
    Ok(())
}

/// 判断一个具体主题是否匹配订阅过滤器（含 `+` / `#` 通配符）
/// 用于 retained 消息投递等场景的离线匹配
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    if topic.is_empty() || filter.is_empty() {
        return false;
    }
    let topic_segs: Vec<&str> = topic.split('/').collect();
    let filter_segs: Vec<&str> = filter.split('/').collect();

    let mut ti = 0usize;
    let mut fi = 0usize;
    while ti < topic_segs.len() && fi < filter_segs.len() {
        let f = filter_segs[fi];
        if f == "#" {
            return true; // 匹配剩余所有层级
        }
        if f == "+" || f == topic_segs[ti] {
            ti += 1;
            fi += 1;
            continue;
        }
        return false;
    }
    // 主题已遍历完，过滤器也必须刚好遍历完（或最后一个为 #）
    if ti == topic_segs.len() {
        if fi == filter_segs.len() {
            return true;
        }
        // 形如 topic=a/b, filter=a/b/# 这种情况
        if fi == filter_segs.len() - 1 && filter_segs[fi] == "#" {
            return true;
        }
    }
    false
}

/// 共享的订阅树句柄
pub type SharedSubscriptionTree = Arc<SubscriptionTree>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_filter_basic() {
        assert!(topic_matches_filter("a/b", "a/b"));
        assert!(topic_matches_filter("a/b", "a/+"));
        assert!(topic_matches_filter("a/b", "+/+"));
        assert!(topic_matches_filter("a/b", "#"));
        assert!(topic_matches_filter("a/b", "a/#"));
        assert!(topic_matches_filter("a/b/c", "a/+/c"));
        assert!(topic_matches_filter("a/b/c", "a/#"));

        assert!(!topic_matches_filter("a/b", "a/c"));
        assert!(!topic_matches_filter("a/b", "b/+"));
        assert!(!topic_matches_filter("a", "a/+"));
        assert!(!topic_matches_filter("a/b/c", "a/b"));
    }

    #[test]
    fn reverse_index_snapshot() {
        let tree = SubscriptionTree::new();
        tree.subscribe("c1", "a/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("c1", "#", QoS::AtMostOnce).unwrap();
        tree.subscribe("c2", "a/b", QoS::ExactlyOnce).unwrap();

        let s1 = tree.subscriptions_of("c1");
        assert_eq!(s1.len(), 2);
        let s2 = tree.subscriptions_of("c2");
        assert_eq!(s2.len(), 1);
        assert_eq!(s2[0].0, "a/b");

        tree.unsubscribe("c1", "a/+").unwrap();
        let s1 = tree.subscriptions_of("c1");
        assert_eq!(s1.len(), 1);
        assert_eq!(s1[0].0, "#");

        tree.unsubscribe_all("c2");
        assert!(tree.subscriptions_of("c2").is_empty());
    }

    #[test]
    fn parse_shared_filter_basic() {
        assert_eq!(parse_shared_filter("$share/g1/a/b"), Some(("g1", "a/b")));
        assert_eq!(parse_shared_filter("$share/g2/a/+"), Some(("g2", "a/+")));
        assert_eq!(parse_shared_filter("$share/g3/#"), Some(("g3", "#")));
        // 无效：组名为空
        assert_eq!(parse_shared_filter("$share//a/b"), None);
        // 无效：非共享前缀
        assert_eq!(parse_shared_filter("a/b"), None);
        assert_eq!(parse_shared_filter("$share/g1"), None); // 缺 filter
    }

    #[test]
    fn shared_subscribe_and_match_picks_one_member() {
        let tree = SubscriptionTree::new();
        // 共享订阅组 g1：两个成员
        tree.subscribe("m1", "$share/g1/a/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m2", "$share/g1/a/+", QoS::AtLeastOnce).unwrap();
        // 普通订阅者
        tree.subscribe("m3", "a/+", QoS::AtMostOnce).unwrap();

        // 匹配 a/b：共享组应只选 1 个成员，普通订阅选 m3
        let shared = tree.matches_shared("a/b");
        assert_eq!(shared.len(), 1, "shared group should pick exactly 1 member");
        let selected_cid = &shared[0].0;
        assert!(selected_cid == "m1" || selected_cid == "m2");

        let normal = tree.matches("a/b");
        assert_eq!(normal.len(), 1);
        assert_eq!(normal[0].0, "m3");
    }

    #[test]
    fn shared_subscribe_round_robin() {
        let tree = SubscriptionTree::new();
        tree.subscribe("m1", "$share/g1/task/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m2", "$share/g1/task/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m3", "$share/g1/task/+", QoS::AtLeastOnce).unwrap();

        // 连续 3 次匹配，应轮询到 3 个不同成员（排序后取模）
        let mut picked: Vec<String> = Vec::new();
        for _ in 0..3 {
            let r = tree.matches_shared("task/x");
            assert_eq!(r.len(), 1);
            picked.push(r[0].0.clone());
        }
        picked.sort();
        assert_eq!(picked, vec!["m1".to_string(), "m2".to_string(), "m3".to_string()]);
    }

    #[test]
    fn shared_unsubscribe_removes_member() {
        let tree = SubscriptionTree::new();
        tree.subscribe("m1", "$share/g1/a/b", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m2", "$share/g1/a/b", QoS::AtLeastOnce).unwrap();

        tree.unsubscribe("m1", "$share/g1/a/b").unwrap();

        let r = tree.matches_shared("a/b");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "m2");
    }

    #[test]
    fn shared_unsubscribe_all_removes_from_groups() {
        let tree = SubscriptionTree::new();
        tree.subscribe("m1", "$share/g1/a/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m1", "$share/g2/b/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m2", "$share/g1/a/+", QoS::AtLeastOnce).unwrap();

        tree.unsubscribe_all("m1");

        // m1 应从 g1 和 g2 中移除；g1 仅剩 m2，g2 应被清理（空组）
        let r1 = tree.matches_shared("a/x");
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].0, "m2");

        let r2 = tree.matches_shared("b/x");
        assert!(r2.is_empty(), "g2 should be removed when empty");
    }

    #[test]
    fn shared_multiple_groups_each_pick_one() {
        let tree = SubscriptionTree::new();
        // 两个独立组都订阅同一 filter
        tree.subscribe("g1a", "$share/g1/data/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("g1b", "$share/g1/data/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("g2a", "$share/g2/data/+", QoS::AtLeastOnce).unwrap();
        tree.subscribe("g2b", "$share/g2/data/+", QoS::AtLeastOnce).unwrap();

        let r = tree.matches_shared("data/x");
        assert_eq!(r.len(), 2, "two groups should each pick one member");
        let cids: Vec<&String> = r.iter().map(|(c, _)| c).collect();
        // 每个组各贡献 1 个
        assert!(cids.iter().any(|c| c.as_str() == "g1a" || c.as_str() == "g1b"));
        assert!(cids.iter().any(|c| c.as_str() == "g2a" || c.as_str() == "g2b"));
    }

    #[test]
    fn shared_wildcard_hash_filter() {
        let tree = SubscriptionTree::new();
        tree.subscribe("m1", "$share/g1/sensor/#", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m2", "$share/g1/sensor/#", QoS::AtLeastOnce).unwrap();

        let r = tree.matches_shared("sensor/a/b/c");
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn shared_subscriber_count_includes_members() {
        let tree = SubscriptionTree::new();
        tree.subscribe("c1", "a/b", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m1", "$share/g1/a/b", QoS::AtLeastOnce).unwrap();
        tree.subscribe("m2", "$share/g1/a/b", QoS::AtLeastOnce).unwrap();

        assert_eq!(tree.subscriber_count(), 3);
    }
}
