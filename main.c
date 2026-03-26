#include "debug.h"
#include "include/raft.h"
#include "include/raft_private.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <malloc.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/time.h>
#include <unistd.h>

#define NP 3
raft_server_t *raft;
int self;
raft_node_t *nodes[NP];
const struct sockaddr_in addrs[NP] = {
    {.sin_family = AF_INET, .sin_addr.s_addr = 0x0100a8c0, .sin_port = 0x2823},
    {.sin_family = AF_INET, .sin_addr.s_addr = 0x0200a8c0, .sin_port = 0x2823},
    {.sin_family = AF_INET, .sin_addr.s_addr = 0x0300a8c0, .sin_port = 0x2823}};
int sockfd;

#define SNAPSHOT_BUF_SIZE (32 * 1024)
static char snapshot_buf[SNAPSHOT_BUF_SIZE];
static int snapshot_len = 0;
static int snapshot_in_progress = 0;

static char recv_snapshot_buf[SNAPSHOT_BUF_SIZE];
static int recv_snapshot_len = 0;
static int recv_snapshot_in_progress = 0;
static raft_index_t recv_snapshot_idx = 0;
static raft_term_t recv_snapshot_term = 0;

static raft_index_t applied_idx = 0;

#define MSG_BUF_SIZE (SNAPSHOT_BUF_SIZE + 256)
static char msg_buf[MSG_BUF_SIZE];
static char recv_buf[MSG_BUF_SIZE];

enum
{
  RAFT_MSG_APPENDENTRIES,
  RAFT_MSG_APPENDENTRIES_RESPONSE,
  RAFT_MSG_REQUESTVOTE,
  RAFT_MSG_REQUESTVOTE_RESPONSE,
  RAFT_MSG_ENTRY,
  RAFT_MSG_ENTRY_RESPONSE,
  RAFT_MSG_SNAPSHOT,
  RAFT_MSG_SNAPSHOT_RESPONSE
};

struct sockaddr_in getdest(void *node)
{
  struct sockaddr_in dest;
  int i;
  for (i = 0; i < NP; i++)
  {
    if (node == nodes[i])
    {
      dest = addrs[i];
      break;
    }
  }
  assert(i != NP);
  return dest;
}

void *getnode(struct sockaddr_in *addr)
{
  void *node;
  int i;
  for (i = 0; i < NP; i++)
  {
    if (addr->sin_addr.s_addr == addrs[i].sin_addr.s_addr &&
        addr->sin_port == addrs[i].sin_port)
    {
      node = nodes[i];
      break;
    }
  }
  if (i == NP)
  {
    for (int j = 0; j < sizeof(struct sockaddr_in); j++)
    {
    }
  }
  fsync(1);
  assert(i != NP);
  return node;
}

int send_requestvote(raft_server_t *raft, void *user_data, raft_node_t *node,
                     raft_requestvote_req_t *msg)
{

  struct sockaddr_in dest = getdest(node);
  int type = RAFT_MSG_REQUESTVOTE;
  size_t pos = 0;

  memcpy(msg_buf, &type, sizeof(int));
  pos += sizeof(int);
  memcpy(msg_buf + pos, msg, sizeof(*msg));
  pos += sizeof(*msg);
  memcpy(msg_buf + pos, &self, sizeof(self));
  pos += sizeof(self);

  sendto(sockfd, msg_buf, pos, 0, (struct sockaddr *)&dest, sizeof(dest));
  return 0;
}

int send_requestvote_response(raft_server_t *raft, void *user_data,
                              raft_node_t *node, raft_requestvote_resp_t *msg)
{
  struct sockaddr_in dest = getdest(node);
  int pos = 0;
  int type = RAFT_MSG_REQUESTVOTE_RESPONSE;
  memcpy(msg_buf, &type, sizeof(int));
  pos += sizeof(int);
  memcpy(msg_buf + pos, msg, sizeof(*msg));
  pos += sizeof(*msg);
  sendto(sockfd, msg_buf, pos, 0, (struct sockaddr *)&dest, sizeof(dest));
  return 0;
}

int send_appendentries_response(raft_server_t *raft, void *user_data,
                                raft_node_t *node,
                                raft_appendentries_resp_t *msg)
{
  struct sockaddr_in dest = getdest(node);
  int pos = 0;
  int type = RAFT_MSG_APPENDENTRIES_RESPONSE;
  memcpy(msg_buf, &type, sizeof(int));
  pos += sizeof(int);
  memcpy(msg_buf + pos, msg, sizeof(*msg));
  pos += sizeof(*msg);
  sendto(sockfd, msg_buf, pos, 0, (struct sockaddr *)&dest, sizeof(dest));
  return 0;
}

int send_appendentries(raft_server_t *raft, void *user_data, raft_node_t *node,
                       raft_appendentries_req_t *msg)
{

  struct sockaddr_in dest = getdest(node);
  size_t pos = 0;
  int type = RAFT_MSG_APPENDENTRIES;

  memcpy(msg_buf, &type, sizeof(int));
  pos += sizeof(int);

  size_t header_size = sizeof(raft_appendentries_req_t) - sizeof(msg->entries);
  memcpy(msg_buf + pos, msg, header_size);
  pos += header_size;

  for (int i = 0; i < msg->n_entries; i++)
  {
    raft_entry_t *entry = msg->entries[i];

    size_t entry_header_size = sizeof(raft_entry_t);
    if (pos + entry_header_size + entry->data_len > MSG_BUF_SIZE)
      return -1;

    memcpy(msg_buf + pos, entry, entry_header_size);
    pos += entry_header_size;
    memcpy(msg_buf + pos, entry->data, entry->data_len);
    pos += entry->data_len;
  }
  sendto(sockfd, msg_buf, pos, 0, (struct sockaddr *)&dest, sizeof(dest));
  return 0;
}

int send_snapshot(raft_server_t *raft, void *user_data, raft_node_t *node,
                  raft_snapshot_req_t *msg)
{
  struct sockaddr_in dest = getdest(node);
  size_t pos = 0;
  int type = RAFT_MSG_SNAPSHOT;

  memcpy(msg_buf, &type, sizeof(int));
  pos += sizeof(int);

  if (pos + sizeof(raft_snapshot_req_t) > MSG_BUF_SIZE)
    return -1;
  memcpy(msg_buf + pos, msg, sizeof(raft_snapshot_req_t));
  pos += sizeof(raft_snapshot_req_t);

  sendto(sockfd, msg_buf, pos, 0, (struct sockaddr *)&dest, sizeof(dest));
  return 0;
}

int applylog(raft_server_t *raft, void *user_data, raft_entry_t *entry,
             raft_index_t entry_idx)
{
  applied_idx = entry_idx;
  return 0;
}

int load_snapshot(raft_server_t *raft, void *user_data,
                  raft_term_t snapshot_term, raft_index_t snapshot_index)
{
  int e = raft_begin_load_snapshot(raft, snapshot_term, snapshot_index);
  if (e != 0)
    return e;

  memcpy(snapshot_buf, recv_snapshot_buf, recv_snapshot_len);
  snapshot_len = recv_snapshot_len;

  raft_end_load_snapshot(raft);
  return 0;
}

int get_snapshot_chunk(raft_server_t *raft, void *user_data, raft_node_t *node,
                       raft_size_t offset, raft_snapshot_chunk_t *chunk)
{
  if (offset >= snapshot_len)
  {
    chunk->len = 0;
    chunk->last_chunk = 1;
    return RAFT_ERR_DONE;
  }

  raft_size_t remaining = snapshot_len - offset;
  chunk->len = remaining < 32000 ? remaining : 32000;
  chunk->data = snapshot_buf + offset;
  chunk->last_chunk = (offset + chunk->len >= snapshot_len);

  return 0;
}

int store_snapshot_chunk(raft_server_t *raft, void *user_data,
                         raft_index_t snapshot_index, raft_size_t offset,
                         raft_snapshot_chunk_t *chunk)
{
  if (offset == 0)
  {
    recv_snapshot_in_progress = 1;
    recv_snapshot_len = 0;
    recv_snapshot_idx = snapshot_index;
  }

  if (!recv_snapshot_in_progress)
    return -1;

  if (offset + chunk->len > SNAPSHOT_BUF_SIZE)
    return -1;

  memcpy(recv_snapshot_buf + offset, chunk->data, chunk->len);

  if (offset + chunk->len > recv_snapshot_len)
    recv_snapshot_len = offset + chunk->len;

  recv_snapshot_term = chunk->last_chunk ? snapshot_index : 0;

  return 0;
}

int clear_snapshot(raft_server_t *raft, void *user_data)
{
  recv_snapshot_in_progress = 0;
  recv_snapshot_len = 0;
  recv_snapshot_idx = 0;
  recv_snapshot_term = 0;
  return 0;
}

int persist_metadata(raft_server_t *raft, void *user_data, raft_term_t term,
                     raft_node_id_t vote)
{
  return 0;
}

raft_time_t timestamp(raft_server_t *raft, void *user_data)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (raft_time_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

__attribute__((noinline)) int poll_msg()
{
  struct sockaddr_in sender_addr = {0};
  socklen_t addr_len = sizeof(sender_addr);
  int recvlen = 0;

  while ((recvlen = recvfrom(sockfd, recv_buf, MSG_BUF_SIZE, 0,
                             (struct sockaddr *)&sender_addr, &addr_len)) > 0)
  {
    void *sender = getnode(&sender_addr);
    int type;
    size_t pos = 0;

    if (recvlen < sizeof(type))
      continue;

    memcpy(&type, recv_buf + pos, sizeof(type));
    pos += sizeof(type);

    switch (type)
    {
      case RAFT_MSG_APPENDENTRIES:
      {
        size_t header_size = sizeof(raft_appendentries_req_t) -
                             sizeof(((raft_appendentries_req_t *)0)->entries);
        if (pos + header_size > recvlen)
          break;

        raft_appendentries_req_t ae;
        memcpy(&ae, recv_buf + pos, header_size);
        pos += header_size;

        ae.entries = NULL;
        if (ae.n_entries > 0)
        {
          ae.entries =
              (raft_entry_t **)malloc(ae.n_entries * sizeof(raft_entry_t *));
          if (!ae.entries)
            break;

          for (int i = 0; i < ae.n_entries; i++)
          {
            if (pos + sizeof(raft_entry_t) > recvlen)
            {
              for (int j = 0; j < i; j++)
              {
                raft_entry_release(ae.entries[j]);
              }
              free(ae.entries);
              ae.entries = NULL;
              break;
            }

            raft_entry_t entry_header;
            memcpy(&entry_header, recv_buf + pos, sizeof(raft_entry_t));
            pos += sizeof(raft_entry_t);

            raft_entry_t *entry = raft_entry_new(entry_header.data_len);
            if (!entry)
            {
              for (int j = 0; j < i; j++)
              {
                raft_entry_release(ae.entries[j]);
              }
              free(ae.entries);
              ae.entries = NULL;
              break;
            }

            entry->term = entry_header.term;
            entry->id = entry_header.id;
            entry->session = entry_header.session;
            entry->type = entry_header.type;

            if (pos + entry_header.data_len > recvlen)
            {
              raft_entry_release(entry);
              for (int j = 0; j < i; j++)
              {
                raft_entry_release(ae.entries[j]);
              }
              free(ae.entries);
              ae.entries = NULL;
              break;
            }
            memcpy(entry->data, recv_buf + pos, entry_header.data_len);
            pos += entry_header.data_len;

            ae.entries[i] = entry;
          }
        }

        if (ae.entries || ae.n_entries == 0)
        {
          raft_appendentries_resp_t response;
          raft_recv_appendentries(raft, (raft_node_t *)sender, &ae, &response);
          send_appendentries_response(raft, NULL, (raft_node_t *)sender,
                                      &response);

          if (ae.entries)
          {
            for (int i = 0; i < ae.n_entries; i++)
            {
              raft_entry_release(ae.entries[i]);
            }
            free(ae.entries);
          }
        }
      }
      break;
      case RAFT_MSG_APPENDENTRIES_RESPONSE:
      {
        if (pos + sizeof(raft_appendentries_resp_t) > recvlen)
          break;
        raft_appendentries_resp_t response;
        memcpy(&response, recv_buf + pos, sizeof(response));
        pos += sizeof(response);
        raft_recv_appendentries_response(raft, (raft_node_t *)sender,
                                         &response);
        break;
      }
      case RAFT_MSG_REQUESTVOTE:
      {
        if (pos + sizeof(raft_requestvote_req_t) > recvlen)
          break;
        raft_requestvote_resp_t response;
        raft_requestvote_req_t requestvote;
        memcpy(&requestvote, recv_buf + pos, sizeof(requestvote));
        pos += sizeof(requestvote);
        raft_recv_requestvote(raft, (raft_node_t *)sender, &requestvote,
                              &response);
        send_requestvote_response(raft, NULL, (raft_node_t *)sender, &response);
        break;
      }
      case RAFT_MSG_REQUESTVOTE_RESPONSE:
      {
        if (pos + sizeof(raft_requestvote_resp_t) > recvlen)
          break;
        raft_requestvote_resp_t response;
        memcpy(&response, recv_buf + pos, sizeof(response));
        pos += sizeof(response);
        raft_recv_requestvote_response(raft, (raft_node_t *)sender, &response);
        break;
      }
      case RAFT_MSG_SNAPSHOT:
      {
        if (pos + sizeof(raft_snapshot_req_t) > recvlen)
          break;

        raft_snapshot_req_t snap_req;
        memcpy(&snap_req, recv_buf + pos, sizeof(snap_req));
        pos += sizeof(snap_req);

        raft_snapshot_resp_t resp;
        raft_recv_snapshot(raft, (raft_node_t *)sender, &snap_req, &resp);
        break;
      }
      case RAFT_MSG_SNAPSHOT_RESPONSE:
      {
        if (pos + sizeof(raft_snapshot_resp_t) > recvlen)
          break;
        raft_snapshot_resp_t resp;
        memcpy(&resp, recv_buf + pos, sizeof(resp));
        pos += sizeof(resp);
        raft_recv_snapshot_response(raft, (raft_node_t *)sender, &resp);
        break;
      }
    }
    (void)pos;
  }
  return 114514;
}

void logging(raft_server_t *s, void *data, const char *buf) { return; }

static char entry_data_storage[256];
static int entry_counter = 0;

void leader_generate_entries()
{
  if (!raft_is_leader(raft))
  {
    return;
  }

  snprintf(entry_data_storage, sizeof(entry_data_storage),
           "entry_%d_from_node_%d", entry_counter, self);

  raft_entry_t *entry = raft_entry_new(strlen(entry_data_storage) + 1);
  memcpy(entry->data, entry_data_storage, strlen(entry_data_storage) + 1);
  entry->id = entry_counter++;
  entry->type = RAFT_LOGTYPE_NORMAL;

  raft_entry_resp_t response;
  int e = raft_recv_entry(raft, entry, &response);

  raft_entry_release(entry);

  if (e != 0)
  {
  }
}

void maybe_create_snapshot()
{
  raft_index_t commit_idx = raft_get_commit_idx(raft);
  raft_index_t snap_idx = raft_get_snapshot_last_idx(raft);

  if (commit_idx - snap_idx >= 2 && !snapshot_in_progress)
  {
    int e = raft_begin_snapshot(raft);
    if (e == 0)
    {
      snapshot_in_progress = 1;

      snprintf(snapshot_buf, SNAPSHOT_BUF_SIZE,
               "snapshot_at_idx_%ld_term_%ld_node_%d", commit_idx,
               raft_get_current_term(raft), self);
      snapshot_len = strlen(snapshot_buf) + 1;

      raft_end_snapshot(raft);
      snapshot_in_progress = 0;
    }
  }
}

int main(int argc, const char *argv[])
{
  self = atoi(argv[1]);

  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  Assert(bind(sockfd, (struct sockaddr *)&addrs[self],
              sizeof(struct sockaddr_in)) == 0,
         "node %s", argv[1]);

  raft = raft_new();

  raft_cbs_t callbacks = {
      .send_requestvote = send_requestvote,
      .send_appendentries = send_appendentries,
      .applylog = applylog,
      .persist_metadata = persist_metadata,
      .send_snapshot = send_snapshot,
      .load_snapshot = load_snapshot,
      .get_snapshot_chunk = get_snapshot_chunk,
      .store_snapshot_chunk = store_snapshot_chunk,
      .clear_snapshot = clear_snapshot,
      .timestamp = NULL, // timestamp,
      .log = logging,
  };
  raft_set_callbacks(raft, &callbacks, NULL);

  nodes[0] = raft_add_node(raft, (void *)&addrs[0], 0, 0 == self);
  nodes[1] = raft_add_node(raft, (void *)&addrs[1], 1, 1 == self);
  nodes[2] = raft_add_node(raft, (void *)&addrs[2], 2, 2 == self);

  struct timeval tv_old = {.tv_sec = 0, .tv_usec = 0};
  while (1)
  {
    struct timeval tv_new;
    gettimeofday(&tv_new, NULL);

    leader_generate_entries();

    raft_periodic_internal(raft, (tv_new.tv_sec - tv_old.tv_sec) * 1000 +
                                     (tv_new.tv_usec - tv_old.tv_usec) / 1000);
    tv_old = tv_new;

    maybe_create_snapshot();

    poll_msg();
  }

  raft_destroy(raft);

  return 0;
}
