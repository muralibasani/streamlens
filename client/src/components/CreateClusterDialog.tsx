import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { insertClusterSchema, type InsertCluster } from "@/lib/api";
import { useCreateCluster } from "@/hooks/use-kafka";
import { useToast } from "@/hooks/use-toast";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Plus, Server } from "lucide-react";

export function CreateClusterDialog() {
  const [open, setOpen] = useState(false);
  const { toast } = useToast();
  const createCluster = useCreateCluster();

  const form = useForm<InsertCluster>({
    resolver: zodResolver(insertClusterSchema),
    defaultValues: {
      name: "",
      bootstrapServers: "",
      schemaRegistryUrl: "",
      connectUrl: "",
      jmxHost: "",
      jmxPort: undefined as number | undefined,
    },
  });

  const onSubmit = (data: InsertCluster) => {
    createCluster.mutate(data, {
      onSuccess: () => {
        setOpen(false);
        form.reset();
        toast({
          title: "Success",
          description: "Cluster added successfully",
        });
      },
      onError: (error) => {
        toast({
          title: "Error",
          description: error.message,
          variant: "destructive",
        });
      },
    });
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button className="gap-2 bg-primary hover:bg-primary/90 text-primary-foreground shadow-lg shadow-primary/20">
          <Plus className="w-4 h-4" />
          Add Cluster
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-[425px] bg-card border-border">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Server className="w-5 h-5 text-primary" />
            Add Kafka Cluster
          </DialogTitle>
          <DialogDescription>
            Enter your cluster connection details to visualize the topology.
          </DialogDescription>
        </DialogHeader>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
            <FormField
              control={form.control}
              name="name"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Cluster Name</FormLabel>
                  <FormControl>
                    <Input placeholder="Production US-East" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="bootstrapServers"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Bootstrap Servers</FormLabel>
                  <FormControl>
                    <Input placeholder="pkc-12345.us-west-2.aws.confluent.cloud:9092" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="schemaRegistryUrl"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Schema Registry URL (Optional)</FormLabel>
                  <FormControl>
                    <Input placeholder="https://..." value={field.value || ''} onChange={field.onChange} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="connectUrl"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Kafka Connect URL (Optional)</FormLabel>
                  <FormControl>
                    <Input placeholder="http://localhost:8083" value={field.value || ''} onChange={field.onChange} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="jmxHost"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>JMX Host (Optional)</FormLabel>
                  <FormControl>
                    <Input placeholder="localhost" value={field.value || ''} onChange={field.onChange} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="jmxPort"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>JMX Port (Optional)</FormLabel>
                  <FormControl>
                    <Input
                      type="number"
                      placeholder="9999"
                      value={field.value ?? ""}
                      onChange={(e) => {
                    const raw = e.target.value;
                    if (raw === "") field.onChange(undefined);
                    else {
                      const n = parseInt(raw, 10);
                      field.onChange(Number.isNaN(n) ? undefined : n);
                    }
                  }}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <div className="flex justify-end gap-3 pt-4">
              <Button type="button" variant="outline" onClick={() => setOpen(false)}>
                Cancel
              </Button>
              <Button 
                type="submit" 
                disabled={createCluster.isPending}
                className="bg-primary hover:bg-primary/90"
              >
                {createCluster.isPending ? "Connecting..." : "Add Cluster"}
              </Button>
            </div>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}
